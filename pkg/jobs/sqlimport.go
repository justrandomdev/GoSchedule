package jobs

import (
	"errors"
	"fmt"
	"jobrunner/pkg/config"
	"jobrunner/pkg/db"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
)

func NewSqlImportPipeline(cons map[string]db.DbConnection, jobConfig config.ImportJobSpec) *SqlImportPipeline {
	dataStruct, nameMap := makeStruct(jobConfig.ColumnMap)
	return &SqlImportPipeline{
		connections:    cons,
		dataStructType: dataStruct,
		job:            jobConfig,
		correctNameMap: nameMap,
		ErrorChan: make(chan error),
	}

}

type SqlImportPipeline struct {
	job            config.ImportJobSpec
	connections    map[string]db.DbConnection
	dataStructType reflect.Type
	correctNameMap map[string]string
	importChan     chan interface{}
	ErrorChan      chan error
}

func (s *SqlImportPipeline) StartImport() error {
	s.importChan = make(chan interface{})
	defer close(s.importChan)
	defer close(s.ErrorChan)

	go s.export()

	//Execute sql query
	db := s.connections[s.job.Connection]
	rows, err := db.Conn.Query(s.job.ImportQuery)
	if err != nil {
		return err
	}

	cols, _ := rows.Columns()

	for rows.Next() {
		instance := makeStructInstance(s.dataStructType)

		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))

		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return err
		}

		for i, colName := range cols {
			val := columnPointers[i].(*interface{})

			if *val == nil {
				continue
			}

			fld := instance.Elem().FieldByName(strings.Title(colName))

			fmt.Println(fld.Interface())
			switch fld.Interface().(type) {
			case string:
				fld.SetString((*val).(string))
			case int:
				intVal, err := strconv.Atoi((*val).(string))
				if err != nil {
					s.ErrorChan<-err
				}
				fld.SetInt(int64(intVal))
			case bool:
				boolVal, err := strconv.ParseBool((*val).(string))
				if err != nil {
					s.ErrorChan<-err
				}
				fld.SetBool(boolVal)
			case time.Time:
				timeVal, err := dateparse.ParseAny((*val).(string))
				if err != nil {
					s.ErrorChan<-err
				}
				fmt.Println(timeVal)
				fld.Set(reflect.ValueOf(timeVal))
			}
		}

		s.importChan<-instance.Interface()
	}

	return nil
}

func (s *SqlImportPipeline) export() {
	for row := range s.importChan {
		fld := reflect.Indirect(reflect.ValueOf(row))

		//query, args, err := sqlx.Named(s.job.ExportQuery, fld)		
		query, err := s.parseNamedQuery(s.job.ExportQuery, fld)
		if err != nil {
			s.ErrorChan<-err
		}

		fmt.Println(query)

		//b.Query(query, args...)
		for i := 0; i < fld.NumField(); i++ {
			name := fld.Type().Field(i).Name
			val := fld.Field(i).Interface()
			fmt.Printf("Field name: %s - value: %+v\n\n", name, val)
		}
	}
}

func (s *SqlImportPipeline) parseNamedQuery(sql string, row interface{}) (string, error) {
	np := regexp.MustCompile(`:(\w)*\b`)
	matches := np.FindAllString(sql, -1)
	for _, v := range matches {
		fldName, found := s.correctNameMap[strings.ReplaceAll(v, ":", "")]
		if !found {
			return "", errors.New("Unknown paramater name in export query: " + v)
		}

		fld := reflect.Indirect(reflect.ValueOf(row))
		field := fld.FieldByName(fldName)
		fldValue := field.Interface()
		fmt.Printf(fldValue.(string))
	}

	return "", nil
}


func makeStruct(columnMap []config.DataField) (reflect.Type, map[string]string) {
	var fields []reflect.StructField
	correctNameMap := map[string]string{}

	//Build struct fields
	for _, val := range columnMap {
		var t reflect.Type

		switch val.Type {
		case "int":
			t = reflect.TypeOf(0)
		case "bool":
			t = reflect.TypeOf(true)
		case "decimal":
			t = reflect.TypeOf(.0)
		case "time":
			t = reflect.TypeOf(time.Now())
		default:
			t = reflect.TypeOf("")
		}

		sf := reflect.StructField{
			Name: strings.Title(val.Name),
			Type: t,
		}

		correctNameMap[val.Name] = sf.Name

		fields = append(fields, sf)
	}

	//Create struct
	return reflect.StructOf(fields), correctNameMap
}

func makeStructInstance(t reflect.Type) reflect.Value {
	instance := reflect.New(t)
	return reflect.ValueOf(instance.Interface())
}

func memUsage(m1, m2 *runtime.MemStats) {
	fmt.Println("Alloc:", m2.Alloc-m1.Alloc,
		"TotalAlloc:", m2.TotalAlloc-m1.TotalAlloc,
		"HeapAlloc:", m2.HeapAlloc-m1.HeapAlloc)
}
