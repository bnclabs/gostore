package storage

type LLRBWriter struct {
	llrb  *LLRB
	reqch chan []interface{}
	finch chan bool
}

func (llrb *LLRB) MVCCWriter() *LLRBWriter {
	if llrb.mvcc.enabled == false {
		panic("cannot create MVCCWriter(), mvcc not enabled")
	}
	chansize := llrb.config["mvcc.writer.chanbuffer"].(int)
	llrb.mvcc.writer = &LLRBWriter{
		llrb:  llrb,
		reqch: make(chan []interface{}, chansize),
		finch: make(chan bool),
	}
	go llrb.mvcc.writer.run()
	return llrb.mvcc.writer
}

const (
	// op commands
	cmdLlrbWriterUpsert byte = iota + 1
	cmdLlrbWriterDeleteMin
	cmdLlrbWriterDeleteMax
	cmdLlrbWriterDelete
	cmdLlrbWriterDestory
	// maintanence commands
	cmdLlrbWriterStatsMem
	cmdLlrbWriterStatsUpsert
	cmdLlrbWriterStatsHeight
	cmdLlrbWriterLogNodeutilz
	cmdLlrbWriterLogNodememory
	cmdLlrbWriterLogValueutilz
	cmdLlrbWriterLogValuememory
	cmdLlrbWriterLogUpsertdepth
	cmdLlrbWriterLogTreeheight

	cmdLlrbWriterClose
)

func (writer *LLRBWriter) Upsert(k, v []byte) (newnd, oldnd *Llrbnode) {
	return nil, nil
}

func (writer *LLRBWriter) DeleteMin() (deleted *Llrbnode) {
	return nil
}

func (writer *LLRBWriter) DeleteMax() (deleted *Llrbnode) {
	return nil
}

func (writer *LLRBWriter) Delete(key []byte) (deleted *Llrbnode) {
	return nil
}

func (writer *LLRBWriter) Destroy() {
}

func (writer *LLRBWriter) StatsMem() map[string]interface{} {
	return nil
}

func (writer *LLRBWriter) StatsUpsert() map[string]interface{} {
	return nil
}

func (writer *LLRBWriter) StatsHeight() map[string]interface{} {
	return nil
}

func (writer *LLRBWriter) LogNodeutilz() {
}

func (writer *LLRBWriter) LogValueutilz() {
}

func (writer *LLRBWriter) LogNodememory() {
}

func (writer *LLRBWriter) LogValuememory() {
}

func (writer *LLRBWriter) LogUpsertdepth() {
}

func (writer *LLRBWriter) LogTreeheight() {
}

func (writer *LLRBWriter) run() {
loop:
	for {
		msg := <-writer.reqch
		switch msg[0].(byte) {
		case cmdLlrbWriterUpsert:
		case cmdLlrbWriterDeleteMin:
		case cmdLlrbWriterDeleteMax:
		case cmdLlrbWriterDelete:
		case cmdLlrbWriterDestory:
		case cmdLlrbWriterStatsMem:
		case cmdLlrbWriterStatsUpsert:
		case cmdLlrbWriterStatsHeight:
		case cmdLlrbWriterLogNodeutilz:
		case cmdLlrbWriterLogNodememory:
		case cmdLlrbWriterLogValueutilz:
		case cmdLlrbWriterLogValuememory:
		case cmdLlrbWriterLogUpsertdepth:
		case cmdLlrbWriterLogTreeheight:
		case cmdLlrbWriterClose:
			break loop
		}
	}
}
