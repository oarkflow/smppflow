package template_test

import (
	"context"
	"testing"

	"github.com/oarkflow/smppflow/pkg/template"
)

func TestStoreSaveDefaultsAndUpdate(t *testing.T) {
	s := template.NewStore()
	ctx := context.Background()
	if err := s.Save(ctx, template.Template{}); err == nil {
		t.Fatal("Save without ID must fail")
	}
	if err := s.Save(ctx, template.Template{ID: "t1", TenantID: "tenant-a", Body: "hi"}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := s.Get(ctx, "t1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != template.Pending {
		t.Fatalf("status=%q want pending", got.Status)
	}
	if got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
		t.Fatal("timestamps must be set")
	}
	if err := s.Save(ctx, template.Template{ID: "t1", TenantID: "tenant-a", Body: "hi2", Status: template.Approved}); err != nil {
		t.Fatal(err)
	}
	got, err = s.Get(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != template.Approved || got.Body != "hi2" {
		t.Fatalf("update not applied: %+v", got)
	}
}

func TestStoreGetMissing(t *testing.T) {
	s := template.NewStore()
	if _, err := s.Get(context.Background(), "nope"); err == nil {
		t.Fatal("Get on missing template must fail")
	}
}

func TestStoreListFiltersAndSorts(t *testing.T) {
	s := template.NewStore()
	ctx := context.Background()
	for _, tpl := range []template.Template{
		{ID: "t2", TenantID: "tenant-a"},
		{ID: "t1", TenantID: "tenant-a"},
		{ID: "t3", TenantID: "tenant-b"},
	} {
		if err := s.Save(ctx, tpl); err != nil {
			t.Fatal(err)
		}
	}
	all, err := s.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 || all[0].ID != "t1" || all[1].ID != "t2" || all[2].ID != "t3" {
		t.Fatalf("all=%v", all)
	}
	filtered, err := s.List(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered) != 2 {
		t.Fatalf("filtered=%v", filtered)
	}
	for _, tpl := range filtered {
		if tpl.TenantID != "tenant-a" {
			t.Fatalf("tenant filter leaked: %+v", tpl)
		}
	}
}

func TestRenderApprovedTemplate(t *testing.T) {
	tpl := template.Template{ID: "otp", Status: template.Approved, Body: "Your OTP is {{.code}}"}
	res, err := template.Render(tpl, map[string]string{"code": "123456"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if res.Text != "Your OTP is 123456" {
		t.Fatalf("text=%q", res.Text)
	}
	if res.TemplateID != "otp" || res.Variables["code"] != "123456" {
		t.Fatalf("result=%+v", res)
	}
	if res.RenderedAt.IsZero() {
		t.Fatal("RenderedAt must be set")
	}
}

func TestRenderRejectsUnapproved(t *testing.T) {
	for _, status := range []template.Status{template.Pending, template.Rejected, template.Expired, ""} {
		tpl := template.Template{ID: "x", Status: status, Body: "hi"}
		if _, err := template.Render(tpl, nil); err == nil {
			t.Fatalf("status=%q must not render", status)
		}
	}
}

func TestRenderMissingVariableFails(t *testing.T) {
	tpl := template.Template{ID: "x", Status: template.Approved, Body: "hi {{.name}}"}
	if _, err := template.Render(tpl, map[string]string{}); err == nil {
		t.Fatal("missing variable must fail with missingkey=error")
	}
}

func TestRenderParseError(t *testing.T) {
	tpl := template.Template{ID: "x", Status: template.Approved, Body: "{{.unclosed"}
	if _, err := template.Render(tpl, nil); err == nil {
		t.Fatal("broken template must fail to parse")
	}
}

func TestRenderExecuteError(t *testing.T) {
	tpl := template.Template{ID: "x", Status: template.Approved, Body: "{{.a.b}}"}
	if _, err := template.Render(tpl, map[string]string{"a": "str"}); err == nil {
		t.Fatal("invalid field access must fail at execute time")
	}
}

func TestExtractVariables(t *testing.T) {
	got := template.ExtractVariables("Hi {{ .name }}, code {{.code}} again {{.name}}")
	want := []string{"code", "name"}
	if len(got) != len(want) {
		t.Fatalf("vars=%v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("vars=%v want %v", got, want)
		}
	}
	if out := template.ExtractVariables("no variables"); len(out) != 0 {
		t.Fatalf("expected no variables, got %v", out)
	}
	if out := template.ExtractVariables("{{}}"); len(out) != 0 {
		t.Fatalf("empty action must yield no variables, got %v", out)
	}
	if out := template.ExtractVariables("{{.a}}{{.a}}"); len(out) != 1 || out[0] != "a" {
		t.Fatalf("duplicates must collapse: %v", out)
	}
}
