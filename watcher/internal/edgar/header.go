package edgar

import (
	"bufio"
	"bytes"
	"strconv"
	"strings"
)

// Every filing's full-submission text file opens with an SGML header:
//
//	<SEC-HEADER>0001234567-24-000069.hdr.sgml : 20240502
//	ACCESSION NUMBER:		0001234567-24-000069
//	CONFORMED SUBMISSION TYPE:	8-K
//	ITEM INFORMATION:		Non-Reliance on Previously Issued Financial Statements or a Related Audit Report or Completed Interim Review
//	FILED AS OF DATE:		20240502
//	...
//		COMPANY CONFORMED NAME:			EXAMPLE CORP
//		CENTRAL INDEX KEY:			0001234567
//
// The header names 8-K items by their heading, not their number, so the two the watcher cares about
// are recognised by the distinctive part of each heading.
var itemHeadings = []struct{ needle, item string }{
	{"non-reliance on previously issued financial statements", "4.02"},
	{"certifying accountant", "4.01"},
}

// ParseHeader reads the SGML header from the start of a submission file. It stops at </SEC-HEADER>,
// so it is safe to hand it only the first few kilobytes of a large filing. ok is false when no
// accession number was found.
func ParseHeader(body []byte) (f Filing, ok bool) {
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var name string
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(line, "</SEC-HEADER>") {
			break
		}
		key, val, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		val = strings.TrimSpace(val)
		switch key {
		case "ACCESSION NUMBER":
			f.Accession = val
		case "CONFORMED SUBMISSION TYPE":
			f.Form = val
		case "FILED AS OF DATE":
			if len(val) == 8 {
				f.Filed = val[:4] + "-" + val[4:6] + "-" + val[6:]
			}
		case "ITEM INFORMATION":
			low := strings.ToLower(val)
			for _, h := range itemHeadings {
				if strings.Contains(low, h.needle) {
					f.Items = appendOnce(f.Items, h.item)
				}
			}
		case "COMPANY CONFORMED NAME":
			name = val
		case "CENTRAL INDEX KEY":
			if cik, err := strconv.ParseInt(val, 10, 64); err == nil && name != "" {
				f.Companies = append(f.Companies, Company{CIK: cik, Name: name})
				name = ""
			}
		}
	}
	return f, ValidAccession(f.Accession)
}
