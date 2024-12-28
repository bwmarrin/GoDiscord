package discordgo

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"
)

var (
	// ErrInteractionExpired is returned when you try to reply to an interaction after 3s
	ErrInteractionExpired = errors.New("interaction expired")

	// ErrInteractionAlreadyRepliedTo is returned when you try to reply to an interaction multiple times
	ErrInteractionAlreadyRepliedTo = errors.New("interaction was already replied to")
)

type replyStatus int

const (
	replyStatusReplied replyStatus = iota + 1
	replyStatusTimedOut
)

// ServeHTTP handles the heavy lifting of parsing the interaction request and sending the response
func (s *Session) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !VerifyInteraction(r, s.PublicKey) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	var i *InteractionCreate

	if err := json.NewDecoder(r.Body).Decode(&i); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
	}

	// we can always respond to ping with pong
	if i.Type == InteractionPing {
		s.log(LogDebug, "received http ping")
		if err := json.NewEncoder(w).Encode(InteractionResponse{
			Type: InteractionResponsePong,
		}); err != nil {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		}
		return
	}

	responseChannel := make(chan *InteractionResponse)
	defer close(responseChannel)
	errorChannel := make(chan error)
	defer close(errorChannel)

	var (
		status replyStatus
		mu     sync.Mutex
	)

	i.Respond = func(response *InteractionResponse) error {
		mu.Lock()
		defer mu.Unlock()

		if status == replyStatusTimedOut {
			return ErrInteractionExpired
		}

		if status == replyStatusReplied {
			return ErrInteractionAlreadyRepliedTo
		}

		status = replyStatusReplied
		responseChannel <- response
		return <-errorChannel
	}

	go s.handleEvent(interactionCreateEventType, i)

	var (
		body        []byte
		contentType string
		err         error
	)

	// interactions can be replied to within 3 seconds, wait 4 to be safe
	timer := time.NewTimer(time.Second * 4)
	defer timer.Stop()
	select {
	case resp := <-responseChannel:
		if resp.Data != nil && len(resp.Data.Files) > 0 {
			contentType, body, err = MultipartBodyWithJSON(resp, resp.Data.Files)
		} else {
			contentType = "application/json"
			body, err = json.Marshal(*resp)
		}
		if err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			errorChannel <- err
			return
		}

	case <-timer.C:
		mu.Lock()
		defer mu.Unlock()
		status = replyStatusTimedOut

		s.log(LogWarning, "interaction timed out")

		http.Error(w, "interaction timed out", http.StatusRequestTimeout)
		return
	}

	w.Header().Set("Content-Type", contentType)
	if _, err = w.Write(body); err != nil {
		errorChannel <- err
	}
}
