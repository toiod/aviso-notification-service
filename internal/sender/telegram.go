package sender

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

type TelegramSender struct {
	token  string
	client *http.Client
}

func NewTelegramSender(token string, client *http.Client) *TelegramSender {
	return &TelegramSender{
		token:  token,
		client: client,
	}
}

type TGMessage struct {
	ChatID string `json:"chat_id"`
	Text   string `json:"text"`
}

type TGResponse struct {
	OK          bool   `json:"ok"`
	Description string `json:"description"`
	ErrorCode   int    `json:"error_code"`
	Parameters  struct {
		RetryAfter int `json:"retry_after"`
	} `json:"parameters"`
}

func (tgs *TelegramSender) Send(ctx context.Context, recipient, message string) error {
	tgMessage := TGMessage{
		ChatID: recipient,
		Text:   message,
	}
	body, err := json.Marshal(tgMessage)
	if err != nil {
		return fmt.Errorf("encode telegram request: %w", err)
	}

	endpoint := "https://api.telegram.org/bot" + tgs.token + "/sendMessage"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create telegram request failed")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := tgs.client.Do(req)
	if err != nil {
		var urlError *url.Error
		if errors.As(err, &urlError) {
			return fmt.Errorf("do send request: %w", urlError.Err)
		}
		return fmt.Errorf("telegram request failed")
	}
	defer resp.Body.Close()
	var tgResponse TGResponse
	decodeErr := json.NewDecoder(resp.Body).Decode(&tgResponse)

	if resp.StatusCode != http.StatusOK {
		description := tgResponse.Description
		if decodeErr != nil {
			description = "invalid Telegram response"
		}
		return &TelegramError{
			HTTPStatus:  resp.StatusCode,
			Code:        tgResponse.ErrorCode,
			Description: description,
			RetryAfter:  tgResponse.Parameters.RetryAfter,
		}
	}

	if decodeErr != nil {
		return fmt.Errorf("decode telegram response: %w", decodeErr)
	}

	if !tgResponse.OK {
		return &TelegramError{
			HTTPStatus:  resp.StatusCode,
			Code:        tgResponse.ErrorCode,
			Description: tgResponse.Description,
			RetryAfter:  tgResponse.Parameters.RetryAfter,
		}
	}

	return nil

}
