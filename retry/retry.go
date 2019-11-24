package retry

// Do will do input function for retryTimes if there is error or return if there is no error
func Do(fn func() error, retryTimes uint32, checkErrSkipRetryFuncs ...func(error) bool) error {
	var err error
	for i := uint32(0); i < retryTimes; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		for _, fn := range checkErrSkipRetryFuncs {
			if fn(err) {
				return err
			}
		}
	}
	return err
}
