package str

func If(cond bool, true, false string) string {
	if cond {
		return true
	}
	return false
}

func Bool(cond bool) string {
	return If(cond, "true", "false")
}
