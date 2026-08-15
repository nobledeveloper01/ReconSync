package account

import (
	"fmt"
	"strings"
)

// A QR encoder, for the one thing that decides whether anyone turns on two-factor
// authentication: whether enrolling means pointing a camera or typing thirty-two
// characters into a phone.
//
// Written by hand for the same reason the PDF writer and /metrics are (§7.3):
// every dependency is something a customer's security team has to approve, and
// this needs one fixed payload shape rendered as squares. Byte mode, error
// correction level M, versions 1 to 10 — an otpauth URI is about 130 bytes, so
// version 8 covers it with room for a long address.
//
// Correctness is not taken on trust: the tests compare the finished module
// matrix, bit for bit, against an independent implementation.

// qrECLevel M is fixed: it tolerates about 15% damage, which is the level every
// authenticator app's own enrolment codes use.
const qrECBits = 0b00 // level M, as it appears in the format information

// qrVersionInfo is the block structure for one version at level M.
type qrVersionInfo struct {
	totalCodewords int
	ecPerBlock     int
	group1Blocks   int
	group1Data     int
	group2Blocks   int
	group2Data     int
	alignment      []int
}

// Versions 1 to 10 at level M. Beyond that the payload is not an otpauth URI and
// the caller should be told so rather than handed a code nothing can read.
var qrVersions = []qrVersionInfo{
	1:  {26, 10, 1, 16, 0, 0, nil},
	2:  {44, 16, 1, 28, 0, 0, []int{6, 18}},
	3:  {70, 26, 1, 44, 0, 0, []int{6, 22}},
	4:  {100, 18, 2, 32, 0, 0, []int{6, 26}},
	5:  {134, 24, 2, 43, 0, 0, []int{6, 30}},
	6:  {172, 16, 4, 27, 0, 0, []int{6, 34}},
	7:  {196, 18, 4, 31, 0, 0, []int{6, 22, 38}},
	8:  {242, 22, 2, 38, 2, 39, []int{6, 24, 42}},
	9:  {292, 22, 3, 36, 2, 37, []int{6, 26, 46}},
	10: {346, 26, 4, 43, 1, 44, []int{6, 28, 50}},
}

// QRMatrix is a finished code: matrix[y][x], true meaning dark.
type QRMatrix [][]bool

// QRCode encodes a payload as a QR matrix.
func QRCode(payload string) (QRMatrix, error) {
	return QRCodeMask(payload, -1)
}

// QRCodeMask encodes with a specific mask, or the best one when mask is
// negative. Fixing the mask is what lets the tests compare a symbol against an
// independent implementation one mask at a time.
func QRCodeMask(payload string, mask int) (QRMatrix, error) {
	data := []byte(payload)

	version, info, err := qrFit(len(data))
	if err != nil {
		return nil, err
	}

	codewords := qrCodewords(data, version, info)
	return qrPlace(version, info, codewords, mask), nil
}

// qrFit picks the smallest version the payload fits in.
func qrFit(n int) (int, qrVersionInfo, error) {
	for version := 1; version <= 10; version++ {
		info := qrVersions[version]
		dataCodewords := info.group1Blocks*info.group1Data + info.group2Blocks*info.group2Data

		// Mode indicator is 4 bits; the character count is 8 bits below
		// version 10 and 16 bits from version 10 up.
		overhead := 4 + qrCountBits(version)
		if (overhead+8*n+7)/8 <= dataCodewords {
			return version, info, nil
		}
	}
	return 0, qrVersionInfo{}, fmt.Errorf("account: %d bytes is too long for a QR code at this size", n)
}

func qrCountBits(version int) int {
	if version < 10 {
		return 8
	}
	return 16
}

// qrCodewords builds the interleaved data and error correction stream.
func qrCodewords(data []byte, version int, info qrVersionInfo) []byte {
	dataCodewords := info.group1Blocks*info.group1Data + info.group2Blocks*info.group2Data

	var bits bitBuffer
	bits.add(0b0100, 4) // byte mode
	bits.add(uint(len(data)), qrCountBits(version))
	for _, b := range data {
		bits.add(uint(b), 8)
	}

	// Terminator, up to four zero bits, then pad to a byte boundary.
	if spare := dataCodewords*8 - bits.len(); spare < 4 {
		bits.add(0, spare)
	} else {
		bits.add(0, 4)
	}
	for bits.len()%8 != 0 {
		bits.add(0, 1)
	}

	// The two alternating pad bytes the standard specifies. Their only job is
	// to fill the capacity with something the masking penalty will not hate.
	padded := bits.bytes()
	for i := 0; len(padded) < dataCodewords; i++ {
		if i%2 == 0 {
			padded = append(padded, 0xEC)
		} else {
			padded = append(padded, 0x11)
		}
	}

	// Split into blocks, each with its own error correction.
	type block struct{ data, ec []byte }
	blocks := make([]block, 0, info.group1Blocks+info.group2Blocks)

	offset := 0
	for i := 0; i < info.group1Blocks; i++ {
		d := padded[offset : offset+info.group1Data]
		offset += info.group1Data
		blocks = append(blocks, block{d, reedSolomon(d, info.ecPerBlock)})
	}
	for i := 0; i < info.group2Blocks; i++ {
		d := padded[offset : offset+info.group2Data]
		offset += info.group2Data
		blocks = append(blocks, block{d, reedSolomon(d, info.ecPerBlock)})
	}

	// Interleaved, so a scratch across the symbol damages a little of every
	// block rather than destroying one outright.
	out := make([]byte, 0, info.totalCodewords)
	maxData := info.group1Data
	if info.group2Data > maxData {
		maxData = info.group2Data
	}
	for i := 0; i < maxData; i++ {
		for _, b := range blocks {
			if i < len(b.data) {
				out = append(out, b.data[i])
			}
		}
	}
	for i := 0; i < info.ecPerBlock; i++ {
		for _, b := range blocks {
			out = append(out, b.ec[i])
		}
	}
	return out
}

// --- bit buffer ---

type bitBuffer struct {
	bits []bool
}

func (b *bitBuffer) add(value uint, n int) {
	for i := n - 1; i >= 0; i-- {
		b.bits = append(b.bits, (value>>uint(i))&1 == 1)
	}
}

func (b *bitBuffer) len() int { return len(b.bits) }

func (b *bitBuffer) bytes() []byte {
	out := make([]byte, (len(b.bits)+7)/8)
	for i, bit := range b.bits {
		if bit {
			out[i/8] |= 1 << uint(7-i%8)
		}
	}
	return out
}

// --- Reed-Solomon over GF(256) ---

var (
	gfExp [512]byte
	gfLog [256]byte
)

func init() {
	// The field QR codes use: primitive polynomial x^8 + x^4 + x^3 + x^2 + 1.
	x := 1
	for i := 0; i < 255; i++ {
		gfExp[i] = byte(x)
		gfLog[x] = byte(i)
		x <<= 1
		if x&0x100 != 0 {
			x ^= 0x11D
		}
	}
	// Doubled, so a product of two logs never needs a modulo.
	for i := 255; i < 512; i++ {
		gfExp[i] = gfExp[i-255]
	}
}

func gfMul(a, b byte) byte {
	if a == 0 || b == 0 {
		return 0
	}
	return gfExp[int(gfLog[a])+int(gfLog[b])]
}

// reedSolomon returns n error correction codewords for the block.
func reedSolomon(data []byte, n int) []byte {
	// The generator polynomial, built as the product of (x - a^i). This loop
	// accumulates it lowest degree first, which is not the order the division
	// below reads it in — hence the reverse. Getting that wrong produces a
	// symbol that is correct in every visible respect and that no scanner can
	// read, because only the error correction is wrong.
	gen := make([]byte, 1, n+1)
	gen[0] = 1
	for i := 0; i < n; i++ {
		gen = append(gen, 0)
		for j := len(gen) - 1; j > 0; j-- {
			gen[j] = gen[j-1] ^ gfMul(gen[j], gfExp[i])
		}
		gen[0] = gfMul(gen[0], gfExp[i])
	}
	for i, j := 0, len(gen)-1; i < j; i, j = i+1, j-1 {
		gen[i], gen[j] = gen[j], gen[i]
	}

	remainder := make([]byte, len(data)+n)
	copy(remainder, data)
	for i := 0; i < len(data); i++ {
		factor := remainder[i]
		if factor == 0 {
			continue
		}
		for j, g := range gen {
			remainder[i+j] ^= gfMul(g, factor)
		}
	}
	return remainder[len(data):]
}

// --- matrix ---

// qrPlace builds the symbol: function patterns, data, then the best mask.
func qrPlace(version int, info qrVersionInfo, codewords []byte, forceMask int) QRMatrix {
	size := version*4 + 17

	m := make(QRMatrix, size)
	reserved := make([][]bool, size)
	for i := range m {
		m[i] = make([]bool, size)
		reserved[i] = make([]bool, size)
	}

	set := func(y, x int, dark bool) {
		m[y][x] = dark
		reserved[y][x] = true
	}

	// Finder patterns and their separators, one per corner but the fourth.
	for _, p := range [][2]int{{0, 0}, {0, size - 7}, {size - 7, 0}} {
		for dy := -1; dy <= 7; dy++ {
			for dx := -1; dx <= 7; dx++ {
				y, x := p[0]+dy, p[1]+dx
				if y < 0 || y >= size || x < 0 || x >= size {
					continue
				}
				inRing := dy == 0 || dy == 6 || dx == 0 || dx == 6
				inCore := dy >= 2 && dy <= 4 && dx >= 2 && dx <= 4
				outside := dy < 0 || dy > 6 || dx < 0 || dx > 6
				set(y, x, !outside && (inRing || inCore))
			}
		}
	}

	// Timing patterns, which tell a scanner the module pitch.
	for i := 8; i < size-8; i++ {
		set(6, i, i%2 == 0)
		set(i, 6, i%2 == 0)
	}

	// Alignment patterns, everywhere two centres meet except under a finder.
	for _, cy := range info.alignment {
		for _, cx := range info.alignment {
			if (cy == 6 && cx == 6) ||
				(cy == 6 && cx == size-7) ||
				(cy == size-7 && cx == 6) {
				continue
			}
			for dy := -2; dy <= 2; dy++ {
				for dx := -2; dx <= 2; dx++ {
					ring := dy == -2 || dy == 2 || dx == -2 || dx == 2
					set(cy+dy, cx+dx, ring || (dy == 0 && dx == 0))
				}
			}
		}
	}

	// The dark module, always set, always here.
	set(size-8, 8, true)

	// Format information areas are reserved now and filled once the mask is
	// chosen, since the mask number is part of what they encode.
	for i := 0; i < 9; i++ {
		if !reserved[8][i] {
			reserved[8][i] = true
		}
		if !reserved[i][8] {
			reserved[i][8] = true
		}
	}
	for i := 0; i < 8; i++ {
		reserved[8][size-1-i] = true
		reserved[size-1-i][8] = true
	}

	// Version information, from version 7 up.
	if version >= 7 {
		bits := qrVersionBits(version)
		for i := 0; i < 18; i++ {
			dark := (bits>>uint(i))&1 == 1
			set(i/3, size-11+i%3, dark)
			set(size-11+i%3, i/3, dark)
		}
	}

	// Data, in a zigzag of two-module columns from bottom right, skipping the
	// vertical timing column.
	bit := 0
	upward := true
	for right := size - 1; right > 0; right -= 2 {
		if right == 6 {
			right = 5 // the timing column is not part of the data path
		}
		for i := 0; i < size; i++ {
			y := i
			if upward {
				y = size - 1 - i
			}
			for _, x := range [2]int{right, right - 1} {
				if reserved[y][x] {
					continue
				}
				if bit < len(codewords)*8 {
					m[y][x] = (codewords[bit/8]>>uint(7-bit%8))&1 == 1
				}
				bit++
			}
		}
		upward = !upward
	}

	// Every mask is applied and scored; the least offensive wins. A scanner
	// copes badly with large same-coloured areas and with anything resembling
	// a finder pattern, which is what the penalties measure.
	if forceMask >= 0 {
		candidate := qrApplyMask(m, reserved, forceMask)
		qrWriteFormat(candidate, forceMask)
		return candidate
	}

	bestScore := -1
	var bestMatrix QRMatrix
	for mask := 0; mask < 8; mask++ {
		candidate := qrApplyMask(m, reserved, mask)
		qrWriteFormat(candidate, mask)
		if score := qrPenalty(candidate); bestScore < 0 || score < bestScore {
			bestScore, bestMatrix = score, candidate
		}
	}
	return bestMatrix
}

func qrApplyMask(src QRMatrix, reserved [][]bool, mask int) QRMatrix {
	out := make(QRMatrix, len(src))
	for y := range src {
		out[y] = make([]bool, len(src[y]))
		copy(out[y], src[y])
		for x := range src[y] {
			if reserved[y][x] {
				continue
			}
			if qrMaskBit(mask, y, x) {
				out[y][x] = !out[y][x]
			}
		}
	}
	return out
}

func qrMaskBit(mask, i, j int) bool {
	switch mask {
	case 0:
		return (i+j)%2 == 0
	case 1:
		return i%2 == 0
	case 2:
		return j%3 == 0
	case 3:
		return (i+j)%3 == 0
	case 4:
		return (i/2+j/3)%2 == 0
	case 5:
		return (i*j)%2+(i*j)%3 == 0
	case 6:
		return ((i*j)%2+(i*j)%3)%2 == 0
	default:
		return ((i+j)%2+(i*j)%3)%2 == 0
	}
}

// qrWriteFormat places the error correction level and mask, twice, so losing
// one corner does not lose the ability to read the symbol at all.
func qrWriteFormat(m QRMatrix, mask int) {
	size := len(m)
	bits := qrFormatBits(qrECBits, mask)

	for i := 0; i < 15; i++ {
		dark := (bits>>uint(i))&1 == 1

		// The first copy runs down column 8 and then left along row 8. The two
		// halves are not mirror images of each other, and writing them as if
		// they were produces a symbol whose finder patterns are perfect and
		// whose contents nothing can read.
		switch {
		case i < 6:
			m[i][8] = dark
		case i == 6:
			m[7][8] = dark
		case i == 7:
			m[8][8] = dark
		case i == 8:
			m[8][7] = dark
		default:
			m[8][14-i] = dark
		}

		// The second, split between the other two finders: the low bits run
		// leftward along row 8 from the right edge, the high bits run down
		// column 8 to the bottom.
		if i < 8 {
			m[8][size-1-i] = dark
		} else {
			m[size-15+i][8] = dark
		}
	}
}

// qrFormatBits is the 15-bit format information: five data bits, a BCH(15,5)
// remainder, then a fixed mask so an all-zero format is never valid.
func qrFormatBits(ec, mask int) int {
	data := ec<<3 | mask
	rem := data << 10
	for i := 14; i >= 10; i-- {
		if rem&(1<<uint(i)) != 0 {
			rem ^= 0b10100110111 << uint(i-10)
		}
	}
	return (data<<10 | rem) ^ 0b101010000010010
}

// qrVersionBits is the 18-bit version information for versions 7 and up.
func qrVersionBits(version int) int {
	rem := version << 12
	for i := 17; i >= 12; i-- {
		if rem&(1<<uint(i)) != 0 {
			rem ^= 0b1111100100101 << uint(i-12)
		}
	}
	return version<<12 | rem
}

// qrPenalty scores a masked symbol; lower is easier to scan.
func qrPenalty(m QRMatrix) int {
	size := len(m)
	score := 0

	// Runs of five or more in a row or column.
	for i := 0; i < size; i++ {
		for _, line := range [2][]bool{qrRow(m, i), qrCol(m, i)} {
			run, prev := 1, line[0]
			for j := 1; j < size; j++ {
				if line[j] == prev {
					run++
					continue
				}
				if run >= 5 {
					score += 3 + run - 5
				}
				run, prev = 1, line[j]
			}
			if run >= 5 {
				score += 3 + run - 5
			}
		}
	}

	// Two-by-two blocks of one colour.
	for y := 0; y < size-1; y++ {
		for x := 0; x < size-1; x++ {
			// Chained rather than each compared to the first, which reads as a
			// typo to the linter and, fairly, to a person.
			a, b := m[y][x], m[y][x+1]
			c, d := m[y+1][x], m[y+1][x+1]
			if a == b && b == c && c == d {
				score += 3
			}
		}
	}

	// Anything that looks like a finder pattern, which is how a scanner finds
	// the symbol in the first place.
	finder := []bool{true, false, true, true, true, false, true}
	quiet := []bool{false, false, false, false}
	for i := 0; i < size; i++ {
		for _, line := range [2][]bool{qrRow(m, i), qrCol(m, i)} {
			for j := 0; j+7 <= size; j++ {
				if !qrEqual(line[j:j+7], finder) {
					continue
				}
				if j >= 4 && qrEqual(line[j-4:j], quiet) {
					score += 40
				}
				if j+11 <= size && qrEqual(line[j+7:j+11], quiet) {
					score += 40
				}
			}
		}
	}

	// A symbol that is mostly one colour, which loses contrast.
	dark := 0
	for _, row := range m {
		for _, v := range row {
			if v {
				dark++
			}
		}
	}
	percent := dark * 100 / (size * size)
	deviation := percent - 50
	if deviation < 0 {
		deviation = -deviation
	}
	score += deviation / 5 * 10
	return score
}

func qrRow(m QRMatrix, i int) []bool { return m[i] }

func qrCol(m QRMatrix, i int) []bool {
	out := make([]bool, len(m))
	for y := range m {
		out[y] = m[y][i]
	}
	return out
}

func qrEqual(a, b []bool) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// QRSVG renders a payload as an SVG.
//
// SVG rather than a PNG because it is text: it costs no image encoder, scales to
// whatever a phone camera wants, and can be inlined in the page rather than
// fetched, which keeps the secret out of a second request.
func QRSVG(payload string) (string, error) {
	m, err := QRCode(payload)
	if err != nil {
		return "", err
	}

	// Four modules of quiet zone. Without it many scanners will not see the
	// symbol at all, however correct the modules are.
	const quiet = 4
	size := len(m) + quiet*2

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" `+
		`shape-rendering="crispEdges" role="img" aria-label="Two-factor enrolment code">`, size, size)
	// White is painted explicitly: a transparent background over a dark page
	// theme inverts the code, and an inverted code does not scan.
	fmt.Fprintf(&b, `<rect width="%d" height="%d" fill="#ffffff"/>`, size, size)

	b.WriteString(`<path fill="#000000" d="`)
	for y, row := range m {
		for x, dark := range row {
			if dark {
				fmt.Fprintf(&b, "M%d %dh1v1h-1z", x+quiet, y+quiet)
			}
		}
	}
	b.WriteString(`"/></svg>`)
	return b.String(), nil
}

// QRDebugCodewords exposes the codeword stream for the conformance tests.
func QRDebugCodewords(payload string) string {
	data := []byte(payload)
	version, info, err := qrFit(len(data))
	if err != nil {
		return err.Error()
	}
	out := ""
	for _, c := range qrCodewords(data, version, info) {
		out += fmt.Sprintf("%02x ", c)
	}
	return out
}
