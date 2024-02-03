package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
)

var GlobalConfig Config

const (
	EXPECTED_EL           = 1000
	FALSE_POSITIVE_RATE   = 0.001
	CMS_EPSILON           = 0.001
	CMS_DELTA             = 0.001
	CACHE_CAP             = 100
	MEMTABLE_SIZE         = 10
	STRUCTURE_TYPE        = "skiplist"
	SKIP_LIST_HEIGHT      = 10
	B_TREE_ORDER          = 3
	TOKEN_NUMBER          = 20
	TOKEN_REFRESH_TIME    = 2
	WAL_PATH              = "logs"
	MAX_ENTRY_SIZE        = 1024
	CRC_SIZE              = 4
	TIMESTAMP_SIZE        = 8
	TOMBSTONE_SIZE        = 1
	KEY_SIZE_SIZE         = 8
	VALUE_SIZE_SIZE       = 8
	CRC_START             = 0
	MAX_LEVELS            = 4
	MAX_BYTES             = 5000
	MAX_TABLES            = 2
	SCALING_FACTOR        = 2
	COMPACTION_ALGORITHM  = "sizeTiered"
	CONDITION             = "tables"
	TIMESTAMP_START       = CRC_START + CRC_SIZE
	TOMBSTONE_START       = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START        = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START      = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START             = VALUE_SIZE_START + VALUE_SIZE_SIZE
	HYPERLOGLOG_PRECISION = 8
	HYPERLOGLOG64BITHASH  = false
	WAL_DATA_SIZE         = 3
	WAL_FILE_SIZE         = 20
	WAL_LOW_WATER_MARK    = 3
	SSTABLE_DEGREE        = 0
	SSTABLE_ALL_IN_ONE    = true
)

type Config struct {
	BloomExpectedElements  int     `json:"bloomExpectedElements"`
	BloomFalsePositiveRate float64 `json:"bloomFalsePositive"`
	CacheCapacity          int     `json:"cacheCapacity"`
	CmsEpsilon             float64 `json:"cmsEpsilon"`
	CmsDelta               float64 `json:"cmsDelta"`
	MemtableSize           uint    `json:"memtableSize"`
	StructureType          string  `json:"structureType"`
	SkipListHeight         int     `json:"skipListHeight"`
	TokenNumber            int     `json:"tokenNumber"`
	TokenRefreshTime       float64 `json:"tokenRefreshTime"`
	WalPath                string  `json:"walPath"`
	MaxEntrySize           int     `json:"maxEntrySize"`
	CrcSize                int     `json:"crcSize"`
	TimestampSize          int     `json:"timestampSize"`
	TombstoneSize          int     `json:"tombstoneSize"`
	KeySizeSize            int     `json:"keySizeSize"`
	ValueSizeSize          int     `json:"valueSizeSize"`
	CrcStart               int     `json:"crcStart"`
	MaxLevels              int     `json:"maxLevels"`
	MaxBytes               int     `json:"maxBytes"`
	MaxTables              int     `json:"maxTables"`
	ScalingFactor          int     `json:"scalingFactor"`
	CompactionAlgorithm    string  `json:"compactionAlgorithm"`
	Condition              string  `json:"condition"`
	TimestampStart         int     `json:"timestampStart"`
	TombstoneStart         int     `json:"tombstoneStart"`
	KeySizeStart           int     `json:"keySizeStart"`
	ValueSizeStart         int     `json:"valueSizeStart"`
	KeyStart               int     `json:"keyStart"`
	BTreeOrder             int     `json:"bTreeOrder"`
	HyperloglogPrecision   int     `json:"HyperloglogPrecision"`
	Hyperloglog64bitHash   bool    `json:"Hyperloglog64bitHash"`
	WalFileSize            int     `json:"WalFileSize"`
	WalDataSize            int     `json:"WalDataSize"`
	WalLowWaterMark        int     `json:"WalLowWaterMark"`
	SStableDegree          int     `json:"SStableDegree"`
	SStableAllInOne        bool    `json:"SStableAllInOne"`
}

func NewConfig(filename string) *Config {
	var config Config
	yamlFile, err := ioutil.ReadFile(filename)
	if err != nil {
		config.BloomExpectedElements = EXPECTED_EL
		config.BloomFalsePositiveRate = FALSE_POSITIVE_RATE
		config.CacheCapacity = CACHE_CAP
		config.CmsDelta = CMS_DELTA
		config.CmsEpsilon = CMS_EPSILON
		config.MemtableSize = MEMTABLE_SIZE
		config.StructureType = STRUCTURE_TYPE
		config.SkipListHeight = SKIP_LIST_HEIGHT
		config.TokenNumber = TOKEN_NUMBER
		config.TokenRefreshTime = TOKEN_REFRESH_TIME
		config.WalPath = WAL_PATH
		config.MaxEntrySize = MAX_ENTRY_SIZE
		config.CrcSize = CRC_SIZE
		config.TimestampSize = TIMESTAMP_SIZE
		config.TombstoneSize = TOMBSTONE_SIZE
		config.KeySizeSize = KEY_SIZE_SIZE
		config.ValueSizeSize = VALUE_SIZE_SIZE
		config.CrcStart = CRC_START
		config.BTreeOrder = B_TREE_ORDER
		config.MaxLevels = MAX_LEVELS
		config.MaxBytes = MAX_BYTES
		config.MaxTables = MAX_TABLES
		config.ScalingFactor = SCALING_FACTOR
		config.CompactionAlgorithm = COMPACTION_ALGORITHM
		config.Condition = CONDITION
		config.TimestampStart = TIMESTAMP_START
		config.TombstoneStart = TOMBSTONE_START
		config.KeySizeStart = KEY_SIZE_START
		config.ValueSizeStart = VALUE_SIZE_START
		config.KeyStart = KEY_START
		config.HyperloglogPrecision = HYPERLOGLOG_PRECISION
		config.Hyperloglog64bitHash = HYPERLOGLOG64BITHASH
		config.WalDataSize = WAL_DATA_SIZE
		config.WalFileSize = WAL_FILE_SIZE
		config.WalLowWaterMark = WAL_LOW_WATER_MARK
		config.SStableDegree = SSTABLE_DEGREE
		config.SStableAllInOne = SSTABLE_ALL_IN_ONE
	} else {
		err = json.Unmarshal(yamlFile, &config)
		if err != nil {
			fmt.Printf("Unmarshal: %v", err)
		}
	}

	return &config
}

func Init() {
	PATH := "config\\config.json"

	GlobalConfig = *NewConfig(PATH)

	if _, err := os.Stat(PATH); errors.Is(err, os.ErrNotExist) {
		f, err := os.Create(PATH)
		defer f.Close()
		if err != nil {
			panic(err)
		}

		out, err := json.Marshal(GlobalConfig)
		if err != nil {
			panic(err)
		}

		f.Write(out)
	}
}
