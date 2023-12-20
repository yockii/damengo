// 本方言包基于gorm v1.9.16开发，需要配合达梦数据库驱动使用
// 推荐连接CASE_SENSITIVE=N的数据库使用，因为gorm中对标识符是否加双引号策略不一
package dm

import (
	"crypto/sha1"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jinzhu/gorm"      // 引入gorm v1包
	_ "github.com/yockii/damengo" // 引入dm数据库驱动包
)

var keyNameRegex = regexp.MustCompile("[^a-zA-Z0-9]+")

type dm struct {
	db gorm.SQLCommon
	gorm.DefaultForeignKeyNamer
}

func init() {
	gorm.DefaultCallback.Create().After("gorm:begin_transaction").Register("dm:set_identity_insert", setIdentityInsert)
	gorm.DefaultCallback.Create().Before("gorm:commit_or_rollback_transaction").Register("dm:turn_off_identity_insert", turnOffIdentityInsert)
	gorm.RegisterDialect("dm", &dm{})
}

func setIdentityInsert(scope *gorm.Scope) {
	if scope.Dialect().GetName() == "dm" {
		for _, field := range scope.PrimaryFields() {
			if _, ok := field.TagSettingsGet("AUTO_INCREMENT"); ok && !field.IsBlank {
				scope.NewDB().Exec(fmt.Sprintf("SET IDENTITY_INSERT %v ON", scope.TableName()))
				scope.InstanceSet("dm:identity_insert_on", true)
			}
		}
	}
}

func turnOffIdentityInsert(scope *gorm.Scope) {
	if scope.Dialect().GetName() == "dm" {
		if _, ok := scope.InstanceGet("dm:identity_insert_on"); ok {
			scope.NewDB().Exec(fmt.Sprintf("SET IDENTITY_INSERT %v OFF", scope.TableName()))
		}
	}
}

func (dm) GetName() string {
	return "dm"
}

func (s *dm) SetDB(db gorm.SQLCommon) {
	s.db = db
}

func (dm) BindVar(i int) string {
	return "?"
}

func (dm) Quote(key string) string {
	return fmt.Sprintf(`"%s"`, key)
}

func (*dm) fieldCanAutoIncrement(field *gorm.StructField) bool {
	if value, ok := field.TagSettingsGet("AUTO_INCREMENT"); ok {
		return strings.ToLower(value) != "false"
	}
	return field.IsPrimaryKey
}

// Get Data Type for DM Dialect
func (s *dm) DataTypeOf(field *gorm.StructField) string {
	var dataValue, sqlType, size, additionalType = gorm.ParseFieldStructForDialect(field, s)

	if sqlType == "" {
		// dm custom type
		reflectType := field.Struct.Type
		for reflectType.Kind() == reflect.Ptr {
			reflectType = reflectType.Elem()
		}
		fieldValue := reflect.Indirect(reflect.New(reflectType))

		if fieldValue.Type().Name() == "DmTimestamp" {
			sqlType = "TIMESTAMP"
		} else if fieldValue.Type().Name() == "DmDecimal" {
			sqlType = "DECIMAL"
		} else if fieldValue.Type().Name() == "DmBlob" {
			sqlType = "BLOB"
		} else if fieldValue.Type().Name() == "DmClob" {
			sqlType = "CLOB"
		} else if fieldValue.Type().Name() == "DmIntervalYM" {
			sqlType = "INTERVAL YEAR TO MONTH"
		} else if fieldValue.Type().Name() == "DmIntervalDT" {
			sqlType = "INTERVAL DAY TO SECOND"
		} else {
			switch dataValue.Kind() {
			case reflect.Bool:
				sqlType = "BIT"
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uintptr:
				if s.fieldCanAutoIncrement(field) {
					field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
					sqlType = "INT IDENTITY(1,1)"
				} else {
					sqlType = "INT"
				}
			case reflect.Int64, reflect.Uint32, reflect.Uint64:
				if s.fieldCanAutoIncrement(field) {
					field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
					sqlType = "BIGINT IDENTITY(1,1)"
				} else {
					sqlType = "BIGINT"
				}
			case reflect.Float32, reflect.Float64:
				sqlType = "DOUBLE"
			case reflect.String:
				if size > 0 && size < 32768 {
					sqlType = fmt.Sprintf("VARCHAR(%d)", size)
				} else {
					sqlType = "CLOB"
				}
			case reflect.Struct:
				if _, ok := dataValue.Interface().(time.Time); ok {
					sqlType = "TIMESTAMP WITH TIME ZONE"
				}
			default:
				if gorm.IsByteArrayOrSlice(dataValue) {
					if size > 0 && size < 32768 {
						sqlType = fmt.Sprintf("VARBINARY(%d)", size)
					} else {
						sqlType = "BLOB"
					}
				}
			}
		}
	}

	if sqlType == "" {
		panic(fmt.Sprintf("invalid sql type %s (%s) in field %s for dm", dataValue.Type().Name(), dataValue.Kind().String(), field.Name))
	}

	if strings.TrimSpace(additionalType) == "" {
		return sqlType
	}
	return fmt.Sprintf("%v %v", sqlType, additionalType)
}

func (s dm) HasIndex(tableName string, indexName string) bool {
	currentDatabase, tableName := s.currentDatabaseAndTable(tableName)
	var sql = `SELECT /*+ MAX_OPT_N_TABLES(5) */ COUNT(DISTINCT OBJ_INDS.NAME) FROM
(SELECT ID FROM SYS.SYSOBJECTS WHERE TYPE$ = 'SCH' AND NAME = ?) USERS,
(SELECT ID, SCHID FROM SYS.SYSOBJECTS WHERE TYPE$ = 'SCHOBJ' AND SUBTYPE$ = 'UTAB' AND NAME = ?) TAB,
(SELECT ID, PID, NAME FROM SYS.SYSOBJECTS WHERE SUBTYPE$='INDEX' AND NAME = ?) OBJ_INDS,
SYS.SYSINDEXES AS INDS, SYS.SYSCOLUMNS AS COLS
WHERE TAB.ID =COLS.ID AND TAB.ID =OBJ_INDS.PID AND INDS.ID=OBJ_INDS.ID AND TAB.SCHID= USERS.ID
AND SF_COL_IS_IDX_KEY(INDS.KEYNUM, INDS.KEYINFO, COLS.COLID)=1;`

	var count int
	s.db.QueryRow(sql, currentDatabase, tableName, indexName).Scan(&count)
	return count > 0
}

func (s dm) RemoveIndex(tableName string, indexName string) error {
	currentDatabase, _ := s.currentDatabaseAndTable(tableName)
	_, err := s.db.Exec(fmt.Sprintf(`DROP INDEX "%s"."%s";`, currentDatabase, indexName))
	return err
}

func (s dm) HasForeignKey(tableName string, foreignKeyName string) bool {
	currentDatabase, tableName := s.currentDatabaseAndTable(tableName)
	var sql = `SELECT /*+ MAX_OPT_N_TABLES(5) */ COUNT(T_REF.REF_CONS_NAME) FROM 
(SELECT T_REF_TAB.NAME AS NAME, T_REF_TAB.SCHNAME AS SCHNAME, T_REF_CONS.FINDEXID AS REFED_ID, 
T_REF_CONS.NAME AS REF_CONS_NAME, SF_GET_INDEX_KEY_SEQ(T_REF_IND.KEYNUM, T_REF_IND.KEYINFO, T_REF_COL.COLID) AS REF_KEYNO,
T_REF_COL.NAME AS REF_COL_NAME, T_REF_CONS.FACTION AS FACTION FROM (SELECT NAME, INDEXID, FINDEXID, TABLEID, FACTION,
CONS.TYPE$ as TYPE FROM SYS.SYSCONS CONS, SYS.SYSOBJECTS OBJECTS WHERE NAME = ? AND CONS.ID = OBJECTS.ID) AS T_REF_CONS,
(SELECT TABS.NAME AS NAME, TABS.ID, SCHEMAS.NAME AS SCHNAME FROM(SELECT ID, PID, NAME FROM SYS.SYSOBJECTS WHERE TYPE$ = 'SCH' AND NAME = ?) SCHEMAS,
(SELECT ID, SCHID, NAME FROM SYS.SYSOBJECTS WHERE TYPE$ = 'SCHOBJ' AND SUBTYPE$ = 'UTAB' AND NAME = ?) TABS 
WHERE SCHEMAS.ID == TABS.SCHID)T_REF_TAB,SYS.SYSINDEXES AS T_REF_IND, (SELECT ID, PID FROM SYS.SYSOBJECTS WHERE SUBTYPE$='INDEX') AS T_REF_INDS_OBJ, 
SYS.SYSCOLUMNS AS T_REF_COL WHERE T_REF_TAB.ID = T_REF_CONS.TABLEID AND T_REF_CONS.TYPE='F' AND T_REF_TAB.ID = T_REF_INDS_OBJ.PID AND 
T_REF_TAB.ID = T_REF_COL.ID AND T_REF_CONS.INDEXID = T_REF_INDS_OBJ.ID AND T_REF_IND.ID = T_REF_INDS_OBJ.ID AND 
SF_COL_IS_IDX_KEY(T_REF_IND.KEYNUM, T_REF_IND.KEYINFO, T_REF_COL.COLID)=1) AS T_REF, 
(SELECT T_REFED_CONS.INDEXID AS REFED_ID, T_REFED_TAB.SCH_NAME AS SCHNAME, T_REFED_TAB.TAB_NAME AS NAME, T_REFED_IND.ID AS REFED_IND_ID,
T_REFED_CONS.NAME AS REFED_CONS_NAME, SF_GET_INDEX_KEY_SEQ(T_REFED_IND.KEYNUM, T_REFED_IND.KEYINFO, T_REFED_COL.COLID) AS REFED_KEYNO,
T_REFED_COL.NAME AS REFED_COL_NAME FROM (SELECT NAME, INDEXID, FINDEXID, TABLEID, FACTION, CONS.TYPE$ as TYPE FROM 
SYS.SYSCONS CONS, SYS.SYSOBJECTS OBJECTS WHERE CONS.ID = OBJECTS.ID) AS T_REFED_CONS, (SELECT TAB.ID AS ID, TAB.NAME AS TAB_NAME,
SCH.NAME AS SCH_NAME FROM SYS.SYSOBJECTS TAB, SYS.SYSOBJECTS SCH WHERE TAB.SUBTYPE$='UTAB' AND SCH.TYPE$='SCH' AND TAB.SCHID=SCH.ID) AS T_REFED_TAB,
SYS.SYSINDEXES AS T_REFED_IND, (SELECT ID, PID, NAME FROM SYS.SYSOBJECTS WHERE SUBTYPE$='INDEX') AS T_REFED_INDS_OBJ, SYS.SYSCOLUMNS AS T_REFED_COL
WHERE T_REFED_TAB.ID = T_REFED_CONS.TABLEID AND T_REFED_CONS.TYPE='P' AND T_REFED_TAB.ID = T_REFED_INDS_OBJ.PID AND 
T_REFED_TAB.ID = T_REFED_COL.ID AND T_REFED_CONS.INDEXID = T_REFED_INDS_OBJ.ID AND T_REFED_IND.ID = T_REFED_INDS_OBJ.ID AND
SF_COL_IS_IDX_KEY(T_REFED_IND.KEYNUM, T_REFED_IND.KEYINFO, T_REFED_COL.COLID)=1) AS T_REFED WHERE 
T_REF.REFED_ID = T_REFED.REFED_ID AND T_REF.REF_KEYNO = T_REFED.REFED_KEYNO;`

	var count int
	s.db.QueryRow(sql, foreignKeyName, currentDatabase, tableName).Scan(&count)
	return count > 0
}

func (s dm) HasTable(tableName string) bool {
	currentDatabase, tableName := s.currentDatabaseAndTable(tableName)
	var sql = `SELECT /*+ MAX_OPT_N_TABLES(5) */ COUNT(TABS.NAME) FROM
(SELECT ID, PID FROM SYS.SYSOBJECTS WHERE TYPE$ = 'SCH' AND NAME = ?) SCHEMAS,
(SELECT ID, SCHID, NAME FROM SYS.SYSOBJECTS WHERE
NAME = ? AND TYPE$ = 'SCHOBJ' AND SUBTYPE$ IN ('UTAB', 'STAB', 'VIEW', 'SYNOM')
AND ((SUBTYPE$ ='UTAB' AND CAST((INFO3 & 0x00FF & 0x003F) AS INT) not in (9, 27, 29, 25, 12, 7, 21, 23, 18, 5))
OR SUBTYPE$ in ('STAB', 'VIEW', 'SYNOM'))) TABS
WHERE TABS.SCHID = SCHEMAS.ID AND SF_CHECK_PRIV_OPT(UID(), CURRENT_USERTYPE(), TABS.ID, SCHEMAS.PID, -1, TABS.ID) = 1;`

	var count int
	s.db.QueryRow(sql, currentDatabase, tableName).Scan(&count)
	return count > 0
}

func (s dm) HasColumn(tableName string, columnName string) bool {
	currentDatabase, tableName := s.currentDatabaseAndTable(tableName)
	var sql = `SELECT /*+ MAX_OPT_N_TABLES(5) */ COUNT(DISTINCT COLS.NAME) FROM
(SELECT ID FROM SYS.SYSOBJECTS WHERE TYPE$ = 'SCH' AND NAME = ?) SCHS,
(SELECT ID, SCHID FROM SYS.SYSOBJECTS WHERE TYPE$ = 'SCHOBJ' AND SUBTYPE$ IN ('UTAB', 'STAB', 'VIEW') AND NAME = ?) TABS,
(SELECT NAME, ID FROM SYS.SYSCOLUMNS WHERE NAME = ?) COLS
WHERE TABS.ID = COLS.ID AND SCHS.ID = TABS.SCHID;`

	var count int
	s.db.QueryRow(sql, currentDatabase, tableName, columnName).Scan(&count)
	return count > 0
}

// gorm上层调用传入参数tableName，columnName一定带双引号
func (s dm) ModifyColumn(tableName string, columnName string, typ string) error {
	_, err := s.db.Exec(fmt.Sprintf(`ALTER TABLE %s MODIFY %v %v`, tableName, columnName, typ))
	return err
}

func (s dm) CurrentDatabase() (name string) {
	s.db.QueryRow("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA');").Scan(&name)
	return
}

func (s dm) LimitAndOffsetSQL(limit, offset interface{}) (sql string, err error) {
	if limit != nil {
		if parsedLimit, err := s.parseInt(limit); err != nil {
			return "", err
		} else if parsedLimit >= 0 {
			sql += fmt.Sprintf(" LIMIT %d", parsedLimit)

			if offset != nil {
				if parsedOffset, err := s.parseInt(offset); err != nil {
					return "", err
				} else if parsedOffset >= 0 {
					sql += fmt.Sprintf(" OFFSET %d", parsedOffset)
				}
			}
		}
	}
	return
}

func (dm) SelectFromDummyTable() string {
	return "FROM DUAL"
}

func (dm) LastInsertIDOutputInterstitial(tableName, columnName string, columns []string) string {
	return ""
}

func (dm) LastInsertIDReturningSuffix(tableName, columnName string) string {
	return ""
}

func (dm) DefaultValueStr() string {
	return "DEFAULT VALUES"
}

func (s dm) BuildKeyName(kind, tableName string, fields ...string) string {
	keyName := fmt.Sprintf("%s_%s_%s", kind, tableName, strings.Join(fields, "_"))
	keyName = keyNameRegex.ReplaceAllString(keyName, "_")
	if utf8.RuneCountInString(keyName) <= 128 {
		return keyName
	}
	h := sha1.New()
	h.Write([]byte(keyName))
	bs := h.Sum(nil)

	// sha1 is 40 characters, keep first 88 characters of destination
	destRunes := []rune(keyNameRegex.ReplaceAllString(fields[0], "_"))
	if len(destRunes) > 88 {
		destRunes = destRunes[:88]
	}

	return fmt.Sprintf("%s%x", string(destRunes), bs)
}

func (s dm) NormalizeIndexAndColumn(indexName, columnName string) (string, string) {
	return indexName, columnName
}

func (dm) parseInt(value interface{}) (int64, error) {
	return strconv.ParseInt(fmt.Sprint(value), 0, 0)
}

func (s *dm) currentDatabaseAndTable(tableName string) (string, string) {
	if strings.Contains(tableName, ".") {
		splitStrings := strings.SplitN(tableName, ".", 2)
		return splitStrings[0], splitStrings[1]
	}
	return s.CurrentDatabase(), tableName
}
