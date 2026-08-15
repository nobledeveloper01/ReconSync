package tests

import (
	"fmt"
	"strings"
	"testing"

	"github.com/nobledeveloper01/ReconSync/internal/account"
)

// The QR encoder is checked by reading its own output back.
//
// A QR code fails silently: a symbol with a transposed format block or a
// Reed-Solomon polynomial assembled in the wrong coefficient order looks
// perfectly correct — finder patterns square, timing crisp — and no scanner on
// earth can read it. Only decoding it catches that, and all three bugs found
// while writing this were of exactly that kind.

func TestQRCodeDecodesBackToThePayload(t *testing.T) {
	payloads := []string{
		"a",
		"HELLO WORLD",
		"otpauth://totp/ReconSync:ops@example.com?algorithm=SHA1&digits=6&" +
			"issuer=ReconSync&period=30&secret=2R4SADFMOKFJO4Y4S4QHJDR6EF3MNXQ4",
		// A long address, because that is what pushes an otpauth URI into a
		// larger version with more than one error correction block.
		"otpauth://totp/ReconSync:a.very.long.name.indeed@a-long-domain.example.com" +
			"?algorithm=SHA1&digits=6&issuer=ReconSync&period=30&secret=2R4SADFMOKFJO4Y4S4QHJDR6EF3MNXQ4",
		strings.Repeat("x", 150),
	}

	for _, payload := range payloads {
		m, err := account.QRCode(payload)
		if err != nil {
			t.Fatalf("QRCode(%.30s…): %v", payload, err)
		}

		got, err := decodeQR(m)
		if err != nil {
			t.Errorf("decoding our own %d-byte symbol: %v", len(payload), err)
			continue
		}
		if got != payload {
			t.Errorf("round trip:\n got  %q\n want %q", got, payload)
		}
	}
}

func TestQRCodeRefusesAPayloadItCannotHold(t *testing.T) {
	// Better a clear error than a symbol that silently drops the end of the
	// secret, which would enrol an authenticator that never produces a valid
	// code and leave nobody able to say why.
	if _, err := account.QRCode(strings.Repeat("x", 4000)); err == nil {
		t.Error("accepted a payload far larger than the largest supported version")
	}
}

func TestQRSVGIsSelfContainedAndOpaque(t *testing.T) {
	svg, err := account.QRSVG("otpauth://totp/ReconSync:ops@example.com?secret=ABCDEFGH")
	if err != nil {
		t.Fatalf("QRSVG: %v", err)
	}
	if !strings.HasPrefix(svg, "<svg") || !strings.HasSuffix(svg, "</svg>") {
		t.Error("not a standalone SVG")
	}
	// A transparent background inverts under a dark page theme, and an inverted
	// code does not scan.
	if !strings.Contains(svg, `fill="#ffffff"`) {
		t.Error("no opaque background; the code would invert on a dark theme")
	}
	// The xmlns is a namespace name, not a fetch; anything that would actually
	// load is what the CSP would block.
	for _, external := range []string{"src=", "<image", "<script", "xlink:href", "url("} {
		if strings.Contains(svg, external) {
			t.Errorf("the SVG contains %q, which the dashboard CSP would block", external)
		}
	}
}

// --- a QR decoder, for the tests only ---

// decodeQR reads a symbol back: locate the function modules, undo the mask the
// format bits name, walk the data in the standard order, and parse the header.
// It verifies the Reed-Solomon syndromes rather than correcting errors — this
// reads a symbol we just wrote, so any error at all is a bug worth failing on.
func decodeQR(m account.QRMatrix) (string, error) {
	size := len(m)
	version := (size - 17) / 4

	reserved := reservedModules(size, version)

	mask, err := readFormat(m, size)
	if err != nil {
		return "", err
	}

	bits := readData(m, reserved, size, mask)
	codewords := make([]byte, len(bits)/8)
	for i := range codewords {
		for j := 0; j < 8; j++ {
			if bits[i*8+j] {
				codewords[i] |= 1 << uint(7-j)
			}
		}
	}

	if err := checkSyndromes(codewords, version); err != nil {
		return "", err
	}
	return parsePayload(deinterleave(codewords, version), version)
}

// blockLayout restates the version table independently of the encoder, so a
// wrong entry there cannot be validated by the same wrong entry here.
type blockLayout struct {
	ecPerBlock int
	g1Blocks   int
	g1Data     int
	g2Blocks   int
	g2Data     int
	alignment  []int
}

var layouts = map[int]blockLayout{
	1:  {10, 1, 16, 0, 0, nil},
	2:  {16, 1, 28, 0, 0, []int{6, 18}},
	3:  {26, 1, 44, 0, 0, []int{6, 22}},
	4:  {18, 2, 32, 0, 0, []int{6, 26}},
	5:  {24, 2, 43, 0, 0, []int{6, 30}},
	6:  {16, 4, 27, 0, 0, []int{6, 34}},
	7:  {18, 4, 31, 0, 0, []int{6, 22, 38}},
	8:  {22, 2, 38, 2, 39, []int{6, 24, 42}},
	9:  {22, 3, 36, 2, 37, []int{6, 26, 46}},
	10: {26, 4, 43, 1, 44, []int{6, 28, 50}},
}

func reservedModules(size, version int) [][]bool {
	r := make([][]bool, size)
	for i := range r {
		r[i] = make([]bool, size)
	}
	mark := func(y, x int) {
		if y >= 0 && y < size && x >= 0 && x < size {
			r[y][x] = true
		}
	}

	for _, p := range [][2]int{{0, 0}, {0, size - 7}, {size - 7, 0}} {
		for dy := -1; dy <= 7; dy++ {
			for dx := -1; dx <= 7; dx++ {
				mark(p[0]+dy, p[1]+dx)
			}
		}
	}
	for i := 0; i < size; i++ {
		mark(6, i)
		mark(i, 6)
	}
	for _, cy := range layouts[version].alignment {
		for _, cx := range layouts[version].alignment {
			if (cy == 6 && cx == 6) || (cy == 6 && cx == size-7) || (cy == size-7 && cx == 6) {
				continue
			}
			for dy := -2; dy <= 2; dy++ {
				for dx := -2; dx <= 2; dx++ {
					mark(cy+dy, cx+dx)
				}
			}
		}
	}
	for i := 0; i < 9; i++ {
		mark(8, i)
		mark(i, 8)
	}
	for i := 0; i < 8; i++ {
		mark(8, size-1-i)
		mark(size-1-i, 8)
	}
	if version >= 7 {
		for i := 0; i < 18; i++ {
			mark(i/3, size-11+i%3)
			mark(size-11+i%3, i/3)
		}
	}
	return r
}

// readFormat recovers the mask number, checking both copies agree.
func readFormat(m account.QRMatrix, size int) (int, error) {
	read := func(copy2 bool) int {
		v := 0
		for i := 0; i < 15; i++ {
			var y, x int
			switch {
			case copy2 && i < 8:
				y, x = 8, size-1-i
			case copy2:
				y, x = size-15+i, 8
			case i < 6:
				y, x = i, 8
			case i == 6:
				y, x = 7, 8
			case i == 7:
				y, x = 8, 8
			case i == 8:
				y, x = 8, 7
			default:
				y, x = 8, 14-i
			}
			if m[y][x] {
				v |= 1 << uint(i)
			}
		}
		return v ^ 0b101010000010010
	}

	a, b := read(false), read(true)
	if a != b {
		return 0, errFmt("the two format copies disagree: %015b and %015b", a, b)
	}
	if ec := a >> 13; ec != 0b00 {
		return 0, errFmt("error correction level is %02b, want 00 (M)", ec)
	}
	return (a >> 10) & 7, nil
}

func maskBit(mask, i, j int) bool {
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

func readData(m account.QRMatrix, reserved [][]bool, size, mask int) []bool {
	var bits []bool
	upward := true
	for right := size - 1; right > 0; right -= 2 {
		if right == 6 {
			right = 5
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
				bits = append(bits, m[y][x] != maskBit(mask, y, x))
			}
		}
		upward = !upward
	}
	return bits
}

// deinterleave undoes the block interleaving and returns the data codewords.
func deinterleave(stream []byte, version int) []byte {
	l := layouts[version]
	sizes := make([]int, 0, l.g1Blocks+l.g2Blocks)
	for i := 0; i < l.g1Blocks; i++ {
		sizes = append(sizes, l.g1Data)
	}
	for i := 0; i < l.g2Blocks; i++ {
		sizes = append(sizes, l.g2Data)
	}

	blocks := make([][]byte, len(sizes))
	pos := 0
	longest := 0
	for _, s := range sizes {
		if s > longest {
			longest = s
		}
	}
	for i := 0; i < longest; i++ {
		for b, s := range sizes {
			if i < s {
				blocks[b] = append(blocks[b], stream[pos])
				pos++
			}
		}
	}

	var out []byte
	for _, b := range blocks {
		out = append(out, b...)
	}
	return out
}

// checkSyndromes verifies every block is an exact Reed-Solomon codeword.
func checkSyndromes(stream []byte, version int) error {
	l := layouts[version]
	sizes := make([]int, 0, l.g1Blocks+l.g2Blocks)
	for i := 0; i < l.g1Blocks; i++ {
		sizes = append(sizes, l.g1Data)
	}
	for i := 0; i < l.g2Blocks; i++ {
		sizes = append(sizes, l.g2Data)
	}

	dataBlocks := make([][]byte, len(sizes))
	pos, longest := 0, 0
	for _, s := range sizes {
		if s > longest {
			longest = s
		}
	}
	for i := 0; i < longest; i++ {
		for b, s := range sizes {
			if i < s {
				dataBlocks[b] = append(dataBlocks[b], stream[pos])
				pos++
			}
		}
	}
	ecBlocks := make([][]byte, len(sizes))
	for i := 0; i < l.ecPerBlock; i++ {
		for b := range sizes {
			ecBlocks[b] = append(ecBlocks[b], stream[pos])
			pos++
		}
	}

	exp, log := gfTables()
	for b := range dataBlocks {
		full := append(append([]byte{}, dataBlocks[b]...), ecBlocks[b]...)
		for i := 0; i < l.ecPerBlock; i++ {
			acc := byte(0)
			for _, c := range full {
				if acc != 0 {
					acc = exp[int(log[acc])+i]
				}
				acc ^= c
			}
			if acc != 0 {
				return errFmt("block %d fails syndrome %d: the error correction is wrong", b, i)
			}
		}
	}
	return nil
}

func gfTables() ([512]byte, [256]byte) {
	var exp [512]byte
	var log [256]byte
	x := 1
	for i := 0; i < 255; i++ {
		exp[i] = byte(x)
		log[x] = byte(i)
		x <<= 1
		if x&0x100 != 0 {
			x ^= 0x11D
		}
	}
	for i := 255; i < 512; i++ {
		exp[i] = exp[i-255]
	}
	return exp, log
}

// parsePayload reads the byte-mode segment back out.
func parsePayload(data []byte, version int) (string, error) {
	var bits []bool
	for _, b := range data {
		for i := 7; i >= 0; i-- {
			bits = append(bits, (b>>uint(i))&1 == 1)
		}
	}
	take := func(n int) int {
		v := 0
		for i := 0; i < n; i++ {
			v <<= 1
			if bits[0] {
				v |= 1
			}
			bits = bits[1:]
		}
		return v
	}

	if mode := take(4); mode != 0b0100 {
		return "", errFmt("mode is %04b, want byte mode 0100", mode)
	}
	countBits := 8
	if version >= 10 {
		countBits = 16
	}
	n := take(countBits)
	if n*8 > len(bits) {
		return "", errFmt("the symbol claims %d bytes but holds %d bits", n, len(bits))
	}

	out := make([]byte, n)
	for i := range out {
		out[i] = byte(take(8))
	}
	return string(out), nil
}

func errFmt(format string, args ...any) error {
	return fmt.Errorf(format, args...)
}
