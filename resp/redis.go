package resp

// https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format
// A one byte flag indicates encoding used to save the Value
const (
	String             byte = 0
	List               byte = 1
	Set                byte = 2
	SortedSet          byte = 3
	Hash               byte = 4
	Zipmap             byte = 9
	Ziplist            byte = 10
	Intset             byte = 11
	SortedSetInZiplist byte = 12
	HashmapInZiplist   byte = 13
	None               byte = 0xFF
)

var TypeString = map[byte]string{
	String:             "string",
	List:               "list",
	Set:                "set",
	SortedSet:          "zset",
	Hash:               "hash",
	Zipmap:             "zmap",
	Ziplist:            "list",
	Intset:             "set",
	SortedSetInZiplist: "list",
	HashmapInZiplist:   "list",
	None:               "none",
}
