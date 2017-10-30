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

	HeartBeat chan bool

	//HeartMonitor
	HeartMonitor chan bool
	watchDogUp   bool
	//mu           *sync.Mutex
	cond      *sync.Cond
	connected bool
	bringUp   bool

	authenticate bool

	errorChan chan error

	done chan bool

	reconAtempts int
}

var apiURL = "wss://ws.cex.io/ws"

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
	log.Info("Pong!!")
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
	log.Debug("Subscribed to: ", action)
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
	//log.Debug("Subscribed to: ", action)
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
	//time.Sleep(time.Second * 30)
	ws.cond.L.Lock()
	log.Info("Watchdog Waiting....")
	ws.cond.Wait()
	log.Info("Watchdog locked...")
	ws.cond.L.Unlock()

	ws.watchDogUp = true
	//time.Sleep(time.Second * 5)
	log.Info("Watchdog is Up")
	beatTime := time.Now()
	go ws.beat()
	for ws.connected {

		select {
		case <-ws.HeartBeat:
			{
				//localElapsed := time.Since(beatTime)
				//log.Debug("HeartBeat elapsed: ", localElapsed.Seconds())
				beatTime = time.Now()

				continue
			}
		case <-ws.HeartMonitor:
			{
				elapsed := time.Since(beatTime)
				//log.Debug("WatchDog elapsed: ", heartMonitor)
				if elapsed.Seconds() > 120 {
					timeOutErr := fmt.Errorf("Watchdog timer expried!!!")
					log.Error(timeOutErr)
					ws.errorChan <- timeOutErr
					return
				}

			}
		case <-ws.done:
			{
				log.Info("Watchdog exiting...")
				return
			}
		}
	}
	log.Debug("WatchDog is DOWN!!")

}

func (ws *API) beat() {
	for ws.connected {
		ws.HeartMonitor <- true
		time.Sleep(1 * time.Second)
	}

}
