package edgar

import (
	"bufio"
	"bytes"
	"strconv"
	"strings"
)

// IndexRow is one line of a daily form index (Archives/edgar/daily-index/form.YYYYMMDD.idx):
//
//	Form Type   Company Name                         CIK         Date Filed  File Name
//	---------------------------------------------------------------------------------
//	8-K         EXAMPLE CORP                         1234567     20240502    edgar/data/1234567/0001234567-24-000069.txt
type IndexRow struct {
	Filing
	Path string // relative to Archives/
}

// ParseFormIndex reads a daily form index. The index has no item numbers: an 8-K row only says an
// 8-K exists, and its header has to be read to learn the items.
func ParseFormIndex(body []byte) []IndexRow {
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	inRows := false
	var rows []IndexRow
	for sc.Scan() {
		line := sc.Text()
		if !inRows {
			inRows = strings.HasPrefix(line, "-----")
			continue
		}
		// Form types ("NT 10-K") and names both contain spaces, so the three fixed fields are read
		// from the right and the rest is split at the first run of two spaces.
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		path, date, cikText := fields[len(fields)-1], fields[len(fields)-2], fields[len(fields)-3]
		cik, err := strconv.ParseInt(cikText, 10, 64)
		if err != nil || len(date) != 8 || !strings.HasSuffix(path, ".txt") {
			continue
		}
		cut := strings.LastIndex(line, date)
		if cut < 0 {
			continue
		}
		head := strings.TrimSpace(line[:cut])
		head = strings.TrimSpace(strings.TrimSuffix(head, cikText))
		form, name, found := strings.Cut(head, "  ")
		if !found {
			continue
		}
		acc := strings.TrimSuffix(path[strings.LastIndex(path, "/")+1:], ".txt")
		if !ValidAccession(acc) {
			continue
		}
		rows = append(rows, IndexRow{Path: path, Filing: Filing{
			Accession: acc, Form: strings.TrimSpace(form), Filed: date[:4] + "-" + date[4:6] + "-" + date[6:],
			Companies: []Company{{CIK: cik, Name: strings.TrimSpace(name)}},
		}})
	}
	return rows
}
