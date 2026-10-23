package worker

import (
	"errors"
	"time"

	"github.com/toiod/aviso-notification-service/internal/sender"
)

type failureKind string

const failureTemporary = "temporary"
const failurePermanent = "permanent"
const failureFatal = "fatal"

func classifySendError(err error) (failureKind, time.Duration) {
	var tgErr *sender.TelegramError
	if !errors.As(err, &tgErr) {
		return failureTemporary, 0
	}
	if tgErr.HTTPStatus == 401 || tgErr.Code == 401 {
		return failureFatal, 0
	}
	if tgErr.HTTPStatus == 429 || tgErr.Code == 429 {
		if tgErr.RetryAfter <= 0 {
			return failureTemporary, 0
		}
		return failureTemporary, time.Duration(tgErr.RetryAfter) * time.Second
	}

	if (tgErr.HTTPStatus >= 400 && tgErr.HTTPStatus <= 499) || (tgErr.Code >= 400 && tgErr.Code <= 499) {
		return failurePermanent, 0
	}
	return failureTemporary, 0
}
