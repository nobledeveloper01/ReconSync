package report

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// A compliance report as a document, because that is the form a regulator is
// sent one in. CSV is what a team works from; this is what gets attached to an
// email and filed.
//
// Written by hand rather than with a PDF library for the same reason /metrics
// is: §7.3 treats every dependency as something a customer's security team must
// approve, and this needs text on a page and nothing else. The output is
// PDF 1.4 with a standard font, which every viewer has supported for twenty
// years.

const (
	pageWidth   = 595.0 // A4 at 72dpi
	pageHeight  = 842.0
	marginLeft  = 48.0
	marginTop   = 60.0
	lineHeight  = 14.0
	bodySize    = 9.0
	headingSize = 16.0
)

// PDF renders the report as a document.
func (r Report) PDF() []byte {
	pages := r.layout()

	var objects [][]byte
	// 1 catalog, 2 pages, 3 font, then one content stream and one page object
	// per page. Object numbers are fixed so the cross-reference table can be
	// built without a second pass.
	firstPage := 4
	pageIDs := make([]int, len(pages))
	for i := range pages {
		pageIDs[i] = firstPage + i*2 + 1
	}

	kids := make([]string, len(pageIDs))
	for i, id := range pageIDs {
		kids[i] = fmt.Sprintf("%d 0 R", id)
	}

	objects = append(objects,
		[]byte("<< /Type /Catalog /Pages 2 0 R >>"),
		[]byte(fmt.Sprintf("<< /Type /Pages /Count %d /Kids [%s] >>",
			len(pages), strings.Join(kids, " "))),
		[]byte("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"))

	for i, content := range pages {
		stream := []byte(content)
		objects = append(objects,
			[]byte(fmt.Sprintf("<< /Length %d >>\nstream\n%s\nendstream", len(stream), stream)),
			[]byte(fmt.Sprintf("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 %.0f %.0f] "+
				"/Resources << /Font << /F1 3 0 R >> >> /Contents %d 0 R >>",
				pageWidth, pageHeight, firstPage+i*2)))
	}

	var out bytes.Buffer
	out.WriteString("%PDF-1.4\n")

	offsets := make([]int, len(objects)+1)
	for i, obj := range objects {
		offsets[i+1] = out.Len()
		fmt.Fprintf(&out, "%d 0 obj\n%s\nendobj\n", i+1, obj)
	}

	xref := out.Len()
	fmt.Fprintf(&out, "xref\n0 %d\n0000000000 65535 f \n", len(objects)+1)
	for i := 1; i <= len(objects); i++ {
		fmt.Fprintf(&out, "%010d 00000 n \n", offsets[i])
	}
	fmt.Fprintf(&out, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n",
		len(objects)+1, xref)

	return out.Bytes()
}

// layout builds the content stream for each page, breaking when one fills.
func (r Report) layout() []string {
	var (
		pages []string
		page  strings.Builder
		y     = pageHeight - marginTop
	)

	newPage := func() {
		if page.Len() > 0 {
			pages = append(pages, page.String())
			page.Reset()
		}
		y = pageHeight - marginTop
	}
	write := func(size float64, x float64, text string) {
		fmt.Fprintf(&page, "BT /F1 %.0f Tf %.0f %.0f Td (%s) Tj ET\n", size, x, y, pdfString(text))
	}
	line := func(size float64, text string) {
		if y < marginTop {
			newPage()
		}
		write(size, marginLeft, text)
		y -= lineHeight
	}

	line(headingSize, "Reversal SLA Compliance Report")
	y -= 6
	line(bodySize, "Tenant: "+r.TenantID)
	line(bodySize, fmt.Sprintf("Period: %s to %s",
		r.From.Format("2006-01-02 15:04 MST"), r.To.Format("2006-01-02 15:04 MST")))
	line(bodySize, "Generated: "+r.GeneratedAt.Format(time.RFC3339))
	line(bodySize, fmt.Sprintf("Reversal deadline: %d seconds", r.ReversalDeadlineSeconds))
	y -= 8

	// The caveats go above the numbers, not in a footnote. A reader who stops
	// after the headline must not have missed that it is a lower bound.
	if r.Incomplete {
		line(bodySize, "INCOMPLETE: "+r.Notice)
		y -= 4
	}
	if r.Truncated {
		line(bodySize, "The itemised list below is capped. The counts are exact.")
		y -= 4
	}

	line(headingSize-4, "Summary")
	line(bodySize, fmt.Sprintf("Transactions: %d    Settled: %d    Detected as orphaned: %d",
		r.Totals.Transactions, r.Totals.Settled, r.Totals.OrphansDetected))
	line(bodySize, fmt.Sprintf("Within deadline: %d    Breached: %d    Outstanding: %d",
		r.Compliance.WithinDeadline, r.Compliance.Breached, r.Compliance.Outstanding))
	if r.Compliance.Rate != nil {
		line(bodySize, fmt.Sprintf("Compliance rate: %.1f%% of concluded reversals",
			*r.Compliance.Rate*100))
	} else {
		// Saying nothing concluded is not the same as saying zero per cent.
		line(bodySize, "Compliance rate: not stated — nothing concluded in this period")
	}
	line(bodySize, fmt.Sprintf("Detection latency: p50 %.1fs  p95 %.1fs  max %.1fs (%d samples)",
		r.Detection.P50, r.Detection.P95, r.Detection.Max, r.Detection.Samples))
	y -= 8

	line(headingSize-4, fmt.Sprintf("Breaches (%d)", len(r.Breaches)))
	if len(r.Breaches) == 0 {
		line(bodySize, "None.")
	}

	// Column headers, and the amount is labelled as minor units.
	//
	// Without the label a reader sees "NGN 5000000" and reads fifty lakh naira
	// rather than fifty thousand — a hundredfold misreading in a document sent
	// to a regulator. The value is not converted because the number of minor
	// units per unit differs by currency and is not something this system
	// tracks; naming the unit is honest, guessing the exponent is not.
	if len(r.Breaches) > 0 {
		write(bodySize, marginLeft, "Transaction")
		write(bodySize, marginLeft+200, "Status")
		write(bodySize, marginLeft+300, "Amount (minor units)")
		write(bodySize, marginLeft+430, "Elapsed")
		y -= lineHeight
	}

	for _, b := range r.Breaches {
		if y < marginTop+lineHeight {
			newPage()
			// Repeated on every page: a reader who turns to page four should
			// not have to go back to page one to know what a column is.
			write(bodySize, marginLeft, "Transaction")
			write(bodySize, marginLeft+200, "Status")
			write(bodySize, marginLeft+300, "Amount (minor units)")
			write(bodySize, marginLeft+430, "Elapsed")
			y -= lineHeight
		}
		write(bodySize, marginLeft, b.TransactionID)
		write(bodySize, marginLeft+200, b.Status)
		write(bodySize, marginLeft+300, b.Currency+" "+strconv.FormatInt(b.AmountMinor, 10))
		write(bodySize, marginLeft+430, strconv.FormatFloat(b.ElapsedSeconds, 'f', 1, 64)+"s")
		y -= lineHeight
	}

	newPage()
	return pages
}

// pdfString escapes a value for a PDF literal string.
//
// The same problem the CSV had: every value here is customer-controlled, and a
// transaction id containing a bracket or a backslash would otherwise close the
// string early and have the rest of it parsed as PDF operators. Non-printable
// bytes are dropped rather than escaped — this is a report, not a transport.
func pdfString(s string) string {
	var out strings.Builder
	for _, r := range s {
		switch {
		case r == '(' || r == ')' || r == '\\':
			out.WriteByte('\\')
			out.WriteRune(r)
		case r < 32 || r > 126:
			// Outside WinAnsi's printable range. A viewer would render this as
			// noise, and guessing an encoding is worse than saying nothing.
			out.WriteByte('?')
		default:
			out.WriteRune(r)
		}
	}
	return out.String()
}
