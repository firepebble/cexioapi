package cexio

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"strings"

	"github.com/gorilla/websocket"
)

//API cex.io websocket API type
type API struct {
	//Key API key
	Key string
	//Secret API secret
	Secret string

	conn                *websocket.Conn
	responseSubscribers map[string]chan subscriberType
	subscriberMutex     sync.Mutex
	orderBookHandlers   map[string]chan bool
	stopDataCollector   bool

	//Dialer used to connect to WebSocket server
	Dialer *websocket.Dialer

	//ReceiveDone send message after Close() initiation
	ReceiveDone chan bool

	//HeartBeat
	HeartBeat chan bool

	//HeartMonitor
	HeartMonitor chan bool
	watchDogUp   bool
	//mu           *sync.Mutex
	cond      *sync.Cond
	connected bool
}

var apiURL = "wss://ws.cex.io/ws"

//NewAPI returns new API instance with default settings
func NewAPI(key string, secret string) *API {

	api := &API{
		Key:                 key,
		Secret:              secret,
		Dialer:              websocket.DefaultDialer,
		responseSubscribers: map[string]chan subscriberType{},
		subscriberMutex:     sync.Mutex{},
		orderBookHandlers:   map[string]chan bool{},
		stopDataCollector:   false,
		ReceiveDone:         make(chan bool),
	}
	locker := &sync.Mutex{}
	api.cond = sync.NewCond(locker)
	api.HeartMonitor = make(chan bool)
	api.HeartBeat = make(chan bool, 100)
	return api
}

//Connect connects to cex.io websocket API server
func (a *API) Connect() error {
	a.cond.L.Lock()
	a.connected = false
	go a.watchDog()
	sub := a.subscribe("connected")
	defer a.unsubscribe("connected")

	conn, _, err := a.Dialer.Dial(apiURL, nil)
	if err != nil {
		return err
	}
	a.conn = conn

	// run response from API server collector
	go a.connectionResponse()

	<-sub //wait for connect response

	// run authentication
	err = a.auth()
	if err != nil {
		return err
	}
	log.Info("Connection complete!!")
	a.connected = true
	a.cond.L.Unlock()
	a.cond.Broadcast()

	return nil
}

//Close closes API connection
func (a *API) Close(ID string) error {
	log.Info("Closing CEXIO Websocket connection...", ID)
	//a.stopDataCollector = true

	err := a.conn.Close()
	if err != nil {
		return err
	}

	go func() {
		a.ReceiveDone <- true
	}()
	log.Info("CEXIO Websocket connection closed!!", ID)
	return nil
}

//Ticker send ticker request
func (a *API) Ticker(cCode1 string, cCode2 string) (*ResponseTicker, error) {
	a.cond.L.Lock()
	for !a.connected {
		a.cond.Wait()
	}
	action := "ticker"
	sub := a.subscribe(action)
	defer a.unsubscribe(action)

	timestamp := time.Now().UnixNano()

	msg := requestTicker{
		E:    action,
		Data: []string{cCode1, cCode2},
		Oid:  fmt.Sprintf("%d_%s:%s", timestamp, cCode1, cCode2),
	}

	err := a.conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	if err != nil {
		myError, _ := fmt.Printf("read deadline:%s\n ", err.Error())
		log.Error(myError)
	}

	err = a.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	if err != nil {
		myError, _ := fmt.Printf("write deadline:%s\n ", err.Error())
		log.Error(myError)
	}

	err = a.conn.WriteJSON(msg)
	if err != nil {
		log.Error("xxx WriteJSON error:", err.Error())
		doRestart := false

		if strings.Contains(err.Error(), "use of closed connection") {
			doRestart = true
			log.Warn("use of closed connection detected, handling error")
		}

		if doRestart {
			log.Warn("restarting conn...")
			a.reconnect()
			log.Warn("Rewriting jsjon...")
			err := a.conn.WriteJSON(msg)
			if err != nil {
				log.Fatal("Could not WriteJSON after reconnection...")
			}
			log.Warn("Rewriting jsjon...done!!")
		} else {
			//a.mu.Unlock()
			log.Error("Con WriteJson: ", err.Error())
			a.cond.L.Unlock()
			return nil, err
		}

	}
	a.cond.L.Unlock()
	/*
		if err != nil {
			log.Error("Error while geting ticker: ", err.Error())
			ws.reconnect()
			ticker, err = ws.api.Ticker(cCode1, cCode2)
		}
	*/

	// wait for response from sever
	respMsg := (<-sub).([]byte)
	resp := &ResponseTicker{}
	err = json.Unmarshal(respMsg, resp)
	if err != nil {
		log.Error("Conn Unmarshal: ", err.Error())
		return nil, err
	}

	// check if authentication was successfull
	if resp.OK != "ok" {
		log.Error("Conn Authentication: ", resp.Data)
		return nil, errors.New(resp.Data.Error)
	}
	return resp, nil
}

//Ticker send ticker request
func (a *API) GetBalance() (*responseGetBalance, error) {
	a.cond.L.Lock()
	action := "get-balance"

	sub := a.subscribe(action)
	defer a.unsubscribe(action)

	timestamp := time.Now().UnixNano()

	msg := requestGetBalance{
		E:    action,
		Data: "",
		Oid:  fmt.Sprintf("%d_%s", timestamp, action),
	}

	err := a.conn.WriteJSON(msg)
	if err != nil {
		a.cond.L.Unlock()
		return nil, err
	}

	// wait for response from sever
	resp := (<-sub).(*responseGetBalance)

	/*
		resp := &responseGetBalance{}
		err = json.Unmarshal(respMsg, resp)
		if err != nil {
			return nil, err
		}
	*/

	// check if authentication was successfull
	if resp.OK != "ok" {
		a.cond.L.Unlock()
		return nil, errors.New(resp.OK)
	}
	a.cond.L.Unlock()
	return resp, nil
}

//OrderBookSubscribe subscribes to order book updates.
//Order book snapshot will come as a first update
func (a *API) OrderBookSubscribe(cCode1 string, cCode2 string, depth int64, handler SubscriptionHandler) (int64, error) {

	action := "order-book-subscribe"

	currencyPair := fmt.Sprintf("%s:%s", cCode1, cCode2)

	subscriptionIdentifier := fmt.Sprintf("%s_%s", action, currencyPair)

	sub := a.subscribe(subscriptionIdentifier)
	defer a.unsubscribe(subscriptionIdentifier)

	timestamp := time.Now().UnixNano()

	req := requestOrderBookSubscribe{
		E:   action,
		Oid: fmt.Sprintf("%d_%s:%s", timestamp, cCode1, cCode2),
		Data: requestOrderBookSubscribeData{
			Pair:      []string{cCode1, cCode2},
			Subscribe: true,
			Depth:     depth,
		},
	}

	err := a.conn.WriteJSON(req)
	if err != nil {
		return 0, err
	}

	bookSnapshot := (<-sub).(*responseOrderBookSubscribe)

	go a.handleOrderBookSubscriptions(bookSnapshot, currencyPair, handler)

	return bookSnapshot.Data.ID, nil
}

func (a *API) handleOrderBookSubscriptions(bookSnapshot *responseOrderBookSubscribe, currencyPair string, handler SubscriptionHandler) {

	quit := make(chan bool)

	subscriptionIdentifier := fmt.Sprintf("md_update_%s", currencyPair)
	a.subscribe(subscriptionIdentifier)
	a.orderBookHandlers[subscriptionIdentifier] = quit

	obData := OrderBookUpdateData{
		ID:        bookSnapshot.Data.ID,
		Pair:      bookSnapshot.Data.Pair,
		Timestamp: bookSnapshot.Data.Timestamp,
		Bids:      bookSnapshot.Data.Bids,
		Asks:      bookSnapshot.Data.Asks,
	}

	// process order book snapshot items before order book updates
	go handler(obData)

	sub, err := a.subscriber(subscriptionIdentifier)
	if err != nil {
		log.Info(err)
		return
	}

	for {
		select {
		case <-quit:
			return
		case m := <-sub:

			resp := m.(*responseOrderBookUpdate)

			obData := OrderBookUpdateData{
				ID:        resp.Data.ID,
				Pair:      resp.Data.Pair,
				Timestamp: resp.Data.Timestamp,
				Bids:      resp.Data.Bids,
				Asks:      resp.Data.Asks,
			}

			go handler(obData)
		}
	}
}

//OrderBookUnsubscribe unsubscribes from order book updates
func (a *API) OrderBookUnsubscribe(cCode1 string, cCode2 string) error {

	action := "order-book-unsubscribe"

	sub := a.subscribe(action)
	defer a.unsubscribe(action)

	timestamp := time.Now().UnixNano()

	req := requestOrderBookUnsubscribe{
		E:   action,
		Oid: fmt.Sprintf("%d_%s:%s", timestamp, cCode1, cCode2),
		Data: orderBookPair{
			Pair: []string{cCode1, cCode2},
		},
	}

	err := a.conn.WriteJSON(req)
	if err != nil {
		return err
	}

	msg := (<-sub).([]byte)

	resp := &responseOrderBookUnsubscribe{}

	err = json.Unmarshal(msg, resp)
	if err != nil {
		return err
	}

	if resp.OK != "ok" {
		return errors.New(resp.Data.Error)
	}

	handlerIdentifier := fmt.Sprintf("md_update_%s:%s", cCode1, cCode2)

	// stop processing book messages
	a.orderBookHandlers[handlerIdentifier] <- true
	delete(a.orderBookHandlers, handlerIdentifier)

	return nil
}

//Ticker send ticker request
func (a *API) InitOhlcvNew(cCode1 string, cCode2 string) (*ResponseTicker, error) {
	action := "init-ohlcv-new"

	sub := a.subscribe(action)
	defer a.unsubscribe(action)

	//timestamp := time.Now().UnixNano()

	pairString := fmt.Sprintf("pair-%s-%s", cCode1, cCode2)

	msg := requestInitOhlcvNew{
		E:     action,
		I:     "1m",
		Rooms: []string{pairString},
	}
	a.cond.L.Lock()
	err := a.conn.WriteJSON(msg)
	if err != nil {
		a.cond.L.Unlock()
		log.Error("Con WriteJson: ", err.Error())
		return nil, err
	}
	a.cond.L.Unlock()
	// wait for response from sever
	fmt.Println("Waiting olhcv response...")
	respMsg := (<-sub).([]byte)
	fmt.Println("Waiting olhcv response...done!!")
	resp := &ResponseTicker{}
	err = json.Unmarshal(respMsg, resp)
	if err != nil {
		//a.cond.L.Unlock()
		log.Error("Conn Unmarshal: ", err.Error())
		return nil, err
	}

	// check if authentication was successfull
	if resp.OK != "ok" {
		//a.cond.L.Unlock()
		log.Error("Conn Authentication: ", err.Error())
		return nil, errors.New(resp.Data.Error)
	}
	return resp, nil
}

func (a *API) ResponseCollector() {
	defer a.Close("ResponseCollector")

	a.stopDataCollector = false

	resp := &responseAction{}

	for a.stopDataCollector == false {
		a.cond.L.Lock()
		for !a.connected {
			log.Debug("DataCollector waiting...")
			a.cond.Wait()
			log.Debug("DataCollector continue...")
		}
		a.cond.L.Unlock()
		_, msg, err := a.conn.ReadMessage()
		if err != nil {
			log.Error("responseCollector, ReadMessage error: ", err.Error())
			//a.reconnect()
			a.cond.L.Lock()
			a.connected = false
			a.cond.L.Unlock()
			a.HeartBeat <- true
			a.reconnect()
			log.Debug("response: reconnect complete")
			//continue
		}

		//Send heart beat
		a.HeartBeat <- true

		err = json.Unmarshal(msg, resp)
		if err != nil {
			log.Errorf("responseCollector: %s\nData: %s\n", err, string(msg))
			continue
		}

		subscriberIdentifier := resp.Action

		switch resp.Action {

		case "ping":
			{

				go a.pong()
				continue
			}

		case "disconnecting":
			{
				log.Info("Disconnecting...")
				log.Info("disconnecting:", string(msg))
				break
			}
		case "order-book-subscribe":
			{

				ob := &responseOrderBookSubscribe{}
				err = json.Unmarshal(msg, ob)
				if err != nil {
					log.Errorf("responseCollector | order-book-subscribe: %s\nData: %s\n", err, string(msg))
					continue
				}

				subscriberIdentifier = fmt.Sprintf("order-book-subscribe_%s", ob.Data.Pair)

				sub, err := a.subscriber(subscriberIdentifier)
				if err != nil {
					log.Error("No response handler for message: %s", string(msg))
					continue // don't know how to handle message so just skip it
				}

				sub <- ob
				continue
			}
		case "md_update":
			{

				ob := &responseOrderBookUpdate{}
				err = json.Unmarshal(msg, ob)
				if err != nil {
					log.Infof("responseCollector | md_update: %s\nData: %s\n", err, string(msg))
					continue
				}

				subscriberIdentifier = fmt.Sprintf("md_update_%s", ob.Data.Pair)

				sub, err := a.subscriber(subscriberIdentifier)
				if err != nil {
					log.Infof("No response handler for message: %s", string(msg))
					continue // don't know how to handle message so just skip it
				}

				sub <- ob
				continue
			}
		case "get-balance":
			{
				ob := &responseGetBalance{}
				err = json.Unmarshal(msg, ob)
				if err != nil {
					log.Infof("responseCollector | get_balance: %s\nData: %s\n", err, string(msg))
					continue
				}

				subscriberIdentifier = "get-balance"

				sub, err := a.subscriber(subscriberIdentifier)
				if err != nil {
					log.Infof("No response handler for message: %s", string(msg))
					continue // don't know how to handle message so just skip it
				}

				sub <- ob
				continue
			}

		default:
			sub, err := a.subscriber(subscriberIdentifier)
			if err != nil {
				log.Errorf("No response handler for message: %s", string(msg))
				continue // don't know how to handle message so just skip it
			}
			//log.Debug("Sending response:", string(msg))
			sub <- msg

		}
	}

}

func (a *API) connectionResponse() {

	resp := &responseAction{}

	for !a.connected {

		_, msg, err := a.conn.ReadMessage()
		if err != nil {
			log.Error("Error while waiting for conection start: ", err.Error())
			return
		}
		err = json.Unmarshal(msg, resp)
		if err != nil {
			log.Fatal("connection start error response: %s\n  Data: %s\n", err, string(msg))
		}

		subscriberIdentifier := resp.Action

		switch resp.Action {

		case "ping":
			{

				a.pong()
				continue
			}

		case "disconnecting":
			{
				log.Info("Disconnecting...")
				log.Info("disconnecting:", string(msg))
				break
			}
		case "connected":
			{
				log.Debug("Conection message detected...")
				sub, err := a.subscriber(subscriberIdentifier)
				if err != nil {
					log.Infof("No response handler for message: %s", string(msg))
					continue // don't know how to handle message so just skip it
				}
				log.Debug("Connection response: ", string(msg))
				sub <- msg
			}

		case "auth":
			log.Debug("Auth message detected...")
			sub, err := a.subscriber(subscriberIdentifier)
			if err != nil {
				log.Infof("No response handler for message: %s", string(msg))
				continue // don't know how to handle message so just skip it
			}
			log.Debug("Connection response: ", string(msg))
			a.connected = true
			sub <- msg
			break

		default:
			{
				log.Fatal("unexpected message recieved: ", string(msg))
			}
		}
	}

}

func (a *API) auth() error {

	action := "auth"

	sub := a.subscribe(action)
	defer a.unsubscribe(action)

	timestamp := time.Now().Unix()

	// build signature string
	s := fmt.Sprintf("%d%s", timestamp, a.Key)

	h := hmac.New(sha256.New, []byte(a.Secret))
	h.Write([]byte(s))

	// generate signed signature string
	signature := hex.EncodeToString(h.Sum(nil))

	// build auth request
	request := requestAuthAction{
		E: action,
		Auth: requestAuthData{
			Key:       a.Key,
			Signature: signature,
			Timestamp: timestamp,
		},
	}

	// send auth request to API server
	err := a.conn.WriteJSON(request)
	if err != nil {
		return err
	}

	// wait for auth response from sever
	respMsg := (<-sub).([]byte)

	resp := &responseAuth{}
	err = json.Unmarshal(respMsg, resp)
	if err != nil {
		return err
	}

	// check if authentication was successfull
	if resp.OK != "ok" || resp.Data.OK != "ok" {
		return errors.New(resp.Data.Error)
	}

	return nil
}

func (a *API) pong() {
	msg := requestPong{"pong"}
	a.cond.L.Lock()
	err := a.conn.WriteJSON(msg)
	a.cond.L.Unlock()
	if err != nil {
		log.Errorf("Error while sending Pong message: %s", err)
	}

}

func (a *API) subscribe(action string) chan subscriberType {
	a.subscriberMutex.Lock()
	defer a.subscriberMutex.Unlock()

	a.responseSubscribers[action] = make(chan subscriberType)

	return a.responseSubscribers[action]
}

func (a *API) unsubscribe(action string) {
	a.subscriberMutex.Lock()
	defer a.subscriberMutex.Unlock()

	delete(a.responseSubscribers, action)
}

func (a *API) subscriber(action string) (chan subscriberType, error) {
	a.subscriberMutex.Lock()
	defer a.subscriberMutex.Unlock()

	sub, ok := a.responseSubscribers[action]
	if ok == false {
		return nil, fmt.Errorf("Subscriber '%s' not found", action)
	}

	return sub, nil
}

func (ws *API) reconnect() {
	log.Warn("Reconecting bot...")
	ws.Connect()
	log.Warn("Bot is back online")
}

func (ws *API) watchDog() {

	ws.watchDogUp = true
	time.Sleep(time.Second * 30)
	log.Info("Watchdog is Up")
	beatTime := time.Now()
	go ws.beat()
	for ws.connected {

		select {
		case <-ws.HeartBeat:
			{
				beatTime = time.Now()
				//log.Debug("HeartBeat!!")
			}
		case <-ws.HeartMonitor:
			{
				elapsed := time.Since(beatTime)
				//log.Debug("WatchDog elapsed: ", heartMonitor)
				if elapsed.Seconds() > 4 {
					log.Error("Watchdog timer expried!!!")
					ws.Close("WatchDog")
					time.Sleep(30 * time.Second)
					beatTime = time.Now()
					log.Debug("WatchDog awaken..")
				}

			}

		}
	}
	log.Debug("WatchDog is DOWN!!")
}

func (ws *API) beat() {

	for {
		ws.HeartMonitor <- true
		time.Sleep(3 * time.Second)
	}

}
