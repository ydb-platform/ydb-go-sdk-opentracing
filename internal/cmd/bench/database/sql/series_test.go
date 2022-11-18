package main

import (
	"testing"
	"text/template"
)

func Test_render(t *testing.T) {
	tests := []struct {
		t    *template.Template
		data interface{}
		want string
	}{
		{
			t: template.Must(template.New("").Funcs(
				template.FuncMap{"N": func(start, end int64) (stream chan int64) {
					stream = make(chan int64)
					go func() {
						for i := start; i <= end; i++ {
							stream <- i
						}
						close(stream)
					}()
					return
				}},
			).Parse(`
				CREATE TABLE {{ .TableName }} (
					YCSB_KEY Text,

					{{- range $i := N 0 .FieldsCount }}
					FIELD{{ $i }} Text,
					{{- end }}
					PRIMARY KEY (YCSB_KEY)
				);
			`)),
			data: struct {
				TableName   string
				FieldsCount int64
			}{
				TableName:   "`/local/test`",
				FieldsCount: 5,
			},
			want: `
				CREATE TABLE ` + "`/local/test`" + ` (
					YCSB_KEY Text,
					FIELD0 Text,
					FIELD1 Text,
					FIELD2 Text,
					FIELD3 Text,
					FIELD4 Text,
					FIELD5 Text,
					PRIMARY KEY (YCSB_KEY)
				);
			`,
		},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			if got := render(tt.t, tt.data); got != tt.want {
				t.Errorf("got:\n\n`%v`\nwant:\n\n`%v`\n", got, tt.want)
			}
		})
	}
}
