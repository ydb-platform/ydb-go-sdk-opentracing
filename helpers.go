package ydb

import (
	"fmt"

	"github.com/opentracing/opentracing-go/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/spans"
)

func fieldToAttribute(field spans.KeyValue) log.Field {
	switch field.Type() {
	case spans.IntType:
		return log.Int(field.Key(), field.IntValue())
	case spans.Int64Type:
		return log.Int64(field.Key(), field.Int64Value())
	case spans.StringType:
		return log.String(field.Key(), field.StringValue())
	case spans.BoolType:
		return log.Bool(field.Key(), field.BoolValue())
	case spans.StringsType:
		return log.Object(field.Key(), field.StringsValue())
	case spans.StringerType:
		return log.String(field.Key(), field.Stringer().String())
	default:
		return log.String(field.Key(), fmt.Sprintf("%v", field.AnyValue()))
	}
}

func fieldsToFields(fields []spans.KeyValue) []log.Field {
	attributes := make([]log.Field, 0, len(fields))

	for _, kv := range fields {
		attributes = append(attributes, fieldToAttribute(kv))
	}

	return attributes
}
