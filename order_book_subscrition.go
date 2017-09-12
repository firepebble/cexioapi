package cexio

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

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
