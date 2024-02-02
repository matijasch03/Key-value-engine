package wal

import (
	"fmt"
	"io"
	"log"
	"os"
	config "projekat_nasp/configWal"
	"projekat_nasp/memTable"
	"projekat_nasp/sstable"
	"sort"
	"strconv"
)

type Wal struct {
	Data               []*WalEntry
	MaxDataSize        uint32
	Path               string
	CurrentFileEntries uint32
	MaxFileSize        uint32
	Prefix             string
	CurrentFilename    uint32
	LowWatermark       uint32
}

func NewWal() *Wal {

	files, _ := os.ReadDir("logs" + string(os.PathSeparator))
	currentFilename := len(files)

	wal := Wal{
		Data:               make([]*WalEntry, 0),
		MaxDataSize:        uint32(config.WAL_DATA_SIZE),
		Path:               "logs",
		CurrentFileEntries: 0,
		MaxFileSize:        uint32(config.WAL_FILE_SIZE),
		Prefix:             "wal.0.0.",
		CurrentFilename:    uint32(currentFilename),
		LowWatermark:       uint32(config.WAL_LOW_WATER_MARK) + 1,
	}
	return &wal

}

func (wal *Wal) Write(key string, value []byte, tombstone byte) *WalEntry {

	if uint32(len(wal.Data)) >= wal.MaxDataSize {
		wal.Dump()
	}

	newWalEntry := NewWalEntry(tombstone)
	newWalEntry.Write(key, value)
	wal.Data = append(wal.Data, newWalEntry)

	return newWalEntry

}

func (wal *Wal) Delete(key string, tombstone byte) {

	newWalEntry := NewWalEntry(tombstone)
	newWalEntry.Write(key, nil)
	wal.Data = append(wal.Data, newWalEntry)

}

func (wal *Wal) Dump() bool {

	currentFile, err := os.OpenFile(wal.Path+string(os.PathSeparator)+wal.Prefix+strconv.Itoa(int(wal.CurrentFilename))+".log", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(wal.Data); i++ {

		currentFile.Seek(0, io.SeekEnd)
		currentFile.Write(wal.Data[i].ToBytes())
		wal.CurrentFileEntries++

		if wal.CurrentFileEntries >= wal.MaxFileSize {
			wal.CurrentFilename++
			currentFile.Close()
			currentFile, _ = os.OpenFile(wal.Path+string(os.PathSeparator)+wal.Prefix+strconv.Itoa(int(wal.CurrentFilename))+".log", os.O_RDWR|os.O_CREATE, 0666)
		}

	}

	wal.CurrentFileEntries = 0
	wal.Data = make([]*WalEntry, 0)
	currentFile.Close()
	return true

}

func (wal *Wal) DeleteSegments() {

	files, _ := os.ReadDir(wal.Path + string(os.PathSeparator))
	fileCount := len(files)

	// sorting files based of their modification time (their creation)
	sort.Slice(files, func(i, j int) bool {
		fileI, _ := files[i].Info()
		fileJ, _ := files[j].Info()
		return fileI.ModTime().Before(fileJ.ModTime())
	})

	if len(files) > int(wal.LowWatermark) {

		for _, file := range files {
			os.Remove(wal.Path + string(os.PathSeparator) + file.Name())
			fileCount--
			if fileCount == int(wal.LowWatermark) {
				break
			}
		}
		files, _ = os.ReadDir(wal.Path + string(os.PathSeparator))

		// sorting files based of modification time (their creation)
		sort.Slice(files, func(i, j int) bool {
			fileI, _ := files[i].Info()
			fileJ, _ := files[j].Info()
			return fileI.ModTime().Before(fileJ.ModTime())
		})

		i := 0
		for _, file := range files {
			os.Rename(wal.Path+string(os.PathSeparator)+file.Name(), wal.Path+string(os.PathSeparator)+wal.Prefix+strconv.Itoa(int(i))+".log")
			i++
		}
	}

}

func (wal *Wal) Recovery(table memTable.MemTablesManager) {

	files, _ := os.ReadDir(wal.Path + string(os.PathSeparator))
	fileCount := len(files)

	for i := 0; i < fileCount; i++ {

		file, _ := os.Open(wal.Path + string(os.PathSeparator) + wal.Prefix + strconv.Itoa(i) + ".log")

		fileInformation, err := file.Stat()
		if err != nil {
			panic(err)
		}

		if fileInformation.Size() == 0 {
			return
		}

		for {
			walEntry, err := ReadWalEntry(file)
			if err == io.EOF {
				file.Close()
				break
			}
			fmt.Println(walEntry.Validate())

			if walEntry.Tombstone == 0 {
				full := table.Add(memTable.NewMemTableEntry(string(walEntry.Key), walEntry.Value, walEntry.Tombstone, walEntry.Timestamp))
				if full != nil {
					sstable.CreateSStable(full, 1) // treba ustanoviti kako se TACNO nazivaju sstable fajlovi
				}
			} else {
				table.Delete(string(walEntry.Key))
			}
		}
	}
}
