package jobs

import (
	"database/sql"
	"errors"
	"fmt"
	"jobrunner/pkg/config"
	"jobrunner/pkg/db"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/araddon/dateparse"
)

const (
	
)

func NewSqlImportPipeline(cons map[string]db.DbConnection, jobConfig config.ImportJobSpec, importDbConn db.DbConnection, exportDbConn db.DbConnection) *SqlImportPipeline {
	dataStruct, nameMap := makeStruct(jobConfig.ColumnMap)

	return &SqlImportPipeline {
		connections:      cons,
		dataStructType:   dataStruct,
		job:              jobConfig,
		correctNameMap:   nameMap,
		importDb:         importDbConn,
		exportDb:         exportDbConn,
		ErrorChan:        make(chan error),
		importChan:       make(chan interface{}),
		rollbackDbChan:   make(chan struct{}),
		cancelDbReadChan: make(chan struct{}),
	}
}

type SqlImportPipeline struct {
	job              config.ImportJobSpec
	connections      map[string]db.DbConnection
	dataStructType   reflect.Type
	correctNameMap   map[string]string
	importDb         db.DbConnection
	exportDb         db.DbConnection
	importChan       chan interface{}
	ErrorChan        chan error
	rollbackDbChan   chan struct{}
	cancelDbReadChan chan struct{}
	dbTx             *sql.Tx
	wg               sync.WaitGroup
}

func (s *SqlImportPipeline) StartImport() error {

	err := s.setupDbTransaction()
	if err != nil {
		return err
	}

	go s.export()

	//Execute sql query
	//db := s.connections[s.job.Connection]
	rows, err := s.importDb.Conn.Query(s.job.ImportQuery)
	if err != nil {
		return err
	}

	//Close rows if there's an error on the import side i.e: abort
	go func() {
		for range s.cancelDbReadChan {
			rows.Close()
		}
	}()

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

			switch fld.Interface().(type) {
			case string:
				fld.SetString((*val).(string))
			case int:
				intVal, ok := (*val).(int64)
				if !ok {
					s.ErrorChan<- errors.New("Unable to convert value to int64")
				}
				fld.SetInt(intVal)
			case bool:
				boolVal, ok := (*val).(bool)
				if !ok {
					s.ErrorChan<- errors.New("Unable to convert value to bool")
				}
				fld.SetBool(boolVal)
			case time.Time:
				timeVal, err := dateparse.ParseAny((*val).(string))
				if err != nil {
					s.ErrorChan<-err
				}
				fld.Set(reflect.ValueOf(timeVal))
			}
		}
		s.wg.Add(1)
		s.importChan<-instance.Interface()
	}

	s.wg.Wait()

	err = s.dbTx.Commit() 

	//commit transaction
	return err
}

func (s *SqlImportPipeline) Close() {
	close(s.rollbackDbChan)
	close(s.cancelDbReadChan)
	close(s.importChan)
	close(s.ErrorChan)
}

func (s *SqlImportPipeline) setupDbTransaction() error {
	tx, err := s.exportDb.Conn.Begin()
	if err != nil {
		return err
	}

	s.dbTx = tx

	go func() {
		for range s.rollbackDbChan {
			err := s.dbTx.Rollback()
			if err != nil {
				s.ErrorChan<-err
			}
		}
	}()

	return nil
}

func (s *SqlImportPipeline) export() {
	for row := range s.importChan {

		query, _, err := s.parseNamedQuery(s.job.ExportQuery, row)
		fmt.Println(query)
		if err != nil {
			s.ErrorChan<-err
			s.rollbackDbChan<-struct{}{}
			s.cancelDbReadChan<-struct{}{}
		}

		_, err =  s.exportDb.Conn.Exec(query)
		if err != nil {
			s.ErrorChan<-err
			s.rollbackDbChan<-struct{}{}
			s.cancelDbReadChan<-struct{}{}
		}

		s.wg.Done()

	}
}

func (s *SqlImportPipeline) parseNamedQuery(sql string, row interface{}) (string, []interface{}, error) {
	result := sql
	np := regexp.MustCompile(`:(\w)*\b`)
	matches := np.FindAllString(sql, -1)
	args := make([]interface{}, len(matches))
	
	for i, v := range matches {
		fldName, found := s.correctNameMap[strings.ReplaceAll(v, ":", "")]
		if !found {
			return "", args, errors.New("Unknown paramater name in export query: " + v)
		}

		field := reflect.Indirect(reflect.ValueOf(row)).FieldByName(fldName)
		strVal, err := s.toString(field.Interface())
		if err != nil {
			return "", args, err
		}

		args[i] = field.Interface()
		result = strings.Replace(result, v, strVal, 1)
	}

	return result, args, nil
}

func (s *SqlImportPipeline) toString(value interface{}) (string, error) {

	switch value.(type) {
	case string:
		return  `'` + value.(string) + `'`, nil
	case int:
		return strconv.Itoa(value.(int)), nil
	case bool:
		return strconv.FormatBool(value.(bool)), nil
	case time.Time:
		t, ok := value.(time.Time)
		if !ok {
			return "", errors.New("Error converting Time to string")
		}

		return `'` + t.Format(time.RFC3339) + `'`, nil
	}

	return "", errors.New(`Unsupported type. Can't convert to string`)
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
