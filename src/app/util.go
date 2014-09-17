package main

const INDEX_KEY_PREFIX string = "|"
const INDEX_KEY_LEN int = len("|")

var INDEX_KEY_START = []byte("|")
var INDEX_KEY_END = []byte{'|', 0xff}

const KEY_PREFIX string = "uid:"

var KEY_START = []byte("uid:")
var KEY_END = []byte{'u', 'i', 'd', ':', 0xff}

func indexKey(key string) string {
	return INDEX_KEY_PREFIX + key
}
