package lsm_tree

import (
	"errors"
	"projekat_nasp/config"
	"math"
	"os"
	"strconv"
	"strings"
)

// Size-tiered kompakcija
func SizeTiered() error {
	maxLevels := config.GlobalConfig.MaxLevels
	if maxLevels < 1 {
		return errors.New("max lsm tree levels must be 1 or more")
	}

	maxTables := config.GlobalConfig.MaxTables
	if maxTables < 1 {
		return errors.New("max tables must be 1 or more")
	}

	maxBytes := config.GlobalConfig.MaxBytes
	if maxBytes < 1024 {
		return errors.New("max bytes must be 1024 or more")
	}

	for lvl := 1; lvl < maxLevels; lvl++ {
		files, err := getLevelFiles(lvl)
		if err != nil {
			return err
		}

		multiplier := math.Pow(float64(config.GlobalConfig.ScalingFactor), float64(lvl-1))
		switch config.GlobalConfig.Condition {
		case "tables":
			if len(files) >= maxTables*int(multiplier) {
				err := compact(files, lvl+1)
				if err != nil {
					return err
				}
			}
		case "bytes":
			bytes, err := getLevelBytes(files)
			if err != nil {
				return err
			}
			if bytes >= maxBytes*int(multiplier) {
				err := compact(files, lvl+1)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func getLevelFiles(level int) ([]string, error) {
	var files []string

	dir, err := os.Open("data/sstable/")
	defer dir.Close()
	if err != nil {
		return nil, err
	}

	fileInfo, err := dir.Readdir(-1)
	if err != nil {
		return nil, err
	}

	for _, file := range fileInfo {
		if strings.Contains(file.Name(), "file") {
			fileLevel := strings.Split(strings.TrimSuffix(file.Name(), ".db"), "_")[2]
			if lvl, _ := strconv.Atoi(fileLevel); lvl == level {
				files = append(files, file.Name())
			}
		}
	}

	return files, nil
}

func compact(files []string, level int) error {
	for i := 1; i < len(files); i += 2 {
		first := "data/sstable/" + files[i-1]
		second := "data/sstable/" + files[i]
		err := MergeTables(first, second, level)
		if err != nil {
			return err
		}

		err = deleteMerkleTree(files[i-1])
		if err != nil {
			return err
		}

		err = deleteMerkleTree(files[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func deleteMerkleTree(tableFileName string) error {
	timestamp := strings.Split(tableFileName, "_")[1]

	err := os.Remove("data/sstable/MetaData_" + timestamp + ".txt")

	return err
}

func getLevelBytes(files []string) (int, error) {
	total := 0

	for _, file := range files {
		fi, err := os.Stat("data/sstable/" + file)
		if err != nil {
			return 0, err
		}

		size := fi.Size()
		total += int(size)
	}

	return total, nil
}