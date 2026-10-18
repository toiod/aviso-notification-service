package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/toiod/aviso-notification-service/internal/notification"
)

type NotificationHandler struct {
	service *notification.Service
}

type createNotificationRequest struct {
	Recipient string `json:"recipient"`
	Channel   string `json:"channel"`
	Message   string `json:"message"`
}

type createNotificationResponse struct {
	ID     int64  `json:"id"`
	Status string `json:"status"`
}

func NewNotificationHandler(service *notification.Service) *NotificationHandler {
	return &NotificationHandler{
		service: service,
	}
}

func (h *NotificationHandler) Create(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1024*64)
	var maxErr *http.MaxBytesError
	var request createNotificationRequest
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&request); err != nil {
		if errors.As(err, &maxErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	var extra any
	err := decoder.Decode(&extra)

	if !errors.Is(err, io.EOF) {
		if errors.As(err, &maxErr) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "body must contain a single JSON value", http.StatusBadRequest)
		return
	}
	id, err := h.service.Create(r.Context(), request.Recipient, request.Channel, request.Message)
	if err != nil {
		if errors.Is(err, notification.ErrValidation) {
			http.Error(w, "validation error", http.StatusBadRequest)
			return
		}
		slog.Error("handler to service create", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	response := createNotificationResponse{
		ID:     id,
		Status: "pending",
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("encode notification response", "error", err)
	}
}
