package main

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/oarkflow/smppflow/pkg/lifecycle"
	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/reports"
	"github.com/oarkflow/smppflow/pkg/storage"
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
