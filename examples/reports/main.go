package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/oarkflow/smppflow/lifecycle"
	"github.com/oarkflow/smppflow/message"
	"github.com/oarkflow/smppflow/reports"
	"github.com/oarkflow/smppflow/storage"
	"time"
)

func main() {
	ctx := context.Background()
	st := storage.NewMemory()
	_ = st.SaveMessage(ctx, &message.Message{ID: "msg-1", TenantID: "tenant", Text: "hello", State: lifecycle.MsgSubmitted, CreatedAt: time.Now(), UpdatedAt: time.Now()})
	var jsonOut, csvOut bytes.Buffer
	_ = reports.WriteJSON(ctx, &jsonOut, st)
	_ = reports.WriteCSV(ctx, &csvOut, st)
	fmt.Println(jsonOut.String())
	fmt.Println(csvOut.String())
}
