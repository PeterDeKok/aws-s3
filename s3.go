package s3

import "peterdekok.nl/gotools/logger"

var log logger.Logger

func init() {
    log = logger.New("s3")
}
