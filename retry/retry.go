package retry

// Do will do input function for retryTimes if there is error or return if there is no error
func Do(fn func() error, retryTimes uint32) error {
	var err error
	for i := uint32(0); i < retryTimes; i++ {
		err = fn()
		if err == nil {
			return nil
		}
	}
	return err
}
