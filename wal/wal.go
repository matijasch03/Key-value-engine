package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	config "projekat_nasp/config"
	"projekat_nasp/memTable"
	"projekat_nasp/sstable"
	"sort"
	"strconv"
)

type Wal struct {
	Data               []*WalEntry
	MaxDataSize        uint32 // number of entries one file can contain
	Path               string
	CurrentFileEntries uint32
	MaxFileSize        uint32 // number of bytes one file is limited to
	Prefix             string
	CurrentFilename    uint32
	LowWatermark       uint32
}

func NewWal() *Wal {

	files, _ := os.ReadDir("logs" + string(os.PathSeparator))
	currentFilename := len(files)

	wal := Wal{
		Path:               "logs",
		CurrentFileEntries: 0,
		MaxDataSize:        uint32(config.WAL_DATA_SIZE),
		MaxFileSize:        uint32(config.WAL_FILE_SIZE),
		Prefix:             "wal.0.0.",
		CurrentFilename:    uint32(currentFilename),
		LowWatermark:       uint32(config.WAL_LOW_WATER_MARK) + 1,
	}
	return &wal

}

func (wal *Wal) Write(key string, value []byte, tombstone byte) *WalEntry {

	newWalEntry := NewWalEntry(tombstone)
	newWalEntry.Write(key, value)

	currentFile, err := os.OpenFile(wal.Path+string(os.PathSeparator)+wal.Prefix+strconv.Itoa(int(wal.CurrentFilename))+".log", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(err)
	}

	fileInfo, err := currentFile.Stat()
	if err != nil {
		log.Fatal(err)
	}

	remainingBytes := len(newWalEntry.ToBytes())
	for remainingBytes > 0 {
		if wal.CurrentFileEntries >= wal.MaxDataSize {
			wal.CurrentFilename++
			currentFile.Close()

			// open a new file
			currentFilePath := wal.Path + string(os.PathSeparator) + wal.Prefix + strconv.Itoa(int(wal.CurrentFilename)) + ".log"
			currentFile, err = os.OpenFile(currentFilePath, os.O_RDWR|os.O_CREATE, 0666)
			if err != nil {
				log.Fatal(err)
			}
			fileInfo, err = currentFile.Stat()
			if err != nil {
				log.Fatal(err)
			}
			// reset the entry count for the new file
			wal.CurrentFileEntries = 0
		}

		// determine how many bytes to write in the current iteration
		writeBytes := min(remainingBytes, int(wal.MaxFileSize)-int(fileInfo.Size()))

		// write the new entry bytes to the current file
		currentFile.Seek(0, io.SeekEnd)
		_, err = currentFile.Write(newWalEntry.ToBytes()[len(newWalEntry.ToBytes())-remainingBytes : len(newWalEntry.ToBytes())-remainingBytes+writeBytes])
		if err != nil {
			log.Fatal(err)
		}

		if writeBytes < remainingBytes {
			// number of bytes written is less than number of bytes remaining -> we exceeded the limit so we create new file
			wal.CurrentFilename++
			currentFile.Close()

			// open a new file
			currentFilePath := wal.Path + string(os.PathSeparator) + wal.Prefix + strconv.Itoa(int(wal.CurrentFilename)) + ".log"
			currentFile, err = os.OpenFile(currentFilePath, os.O_RDWR|os.O_CREATE, 0666)
			if err != nil {
				log.Fatal(err)
			}

			wal.CurrentFileEntries = 0

			fileInfo, err = currentFile.Stat()
			if err != nil {
				log.Fatal(err)
			}
		}

		// update counters and loop variables
		remainingBytes -= writeBytes
		wal.CurrentFileEntries++
	}

	currentFile.Close()

	return newWalEntry

}

func (wal *Wal) Delete(key string, tombstone byte) {

	newWalEntry := NewWalEntry(tombstone)
	newWalEntry.Write(key, nil)
	wal.Write(key, nil, tombstone)

}

/* func (wal *Wal) Dump() bool {

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

} */

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

func (wal *Wal) Recovery(table *memTable.MemTablesManager) {
	files, _ := os.ReadDir(wal.Path + string(os.PathSeparator))
	fileCount := len(files)

	var partialEntry []byte
	var entryBytes []byte

	for i := 0; i < fileCount; i++ {
		file, _ := os.Open(wal.Path + string(os.PathSeparator) + wal.Prefix + strconv.Itoa(i) + ".log")
		fileInformation, err := file.Stat()
		if err != nil {
			panic(err)
		}

		if fileInformation.Size() == 0 {
			file.Close()
			continue
		}

		for {
			// read the entire file
			entryFileBytes := make([]byte, wal.MaxFileSize)
			n, err := file.Read(entryFileBytes)
			if err == io.EOF {
				file.Close()
				break
			}
			// combine with any remaining partial entry
			partialEntry = append(partialEntry, entryFileBytes[:n]...)
			entryBytes = partialEntry
			// process complete entries
			for len(entryBytes) >= 29 {
				// extract the first 29 bytes
				headerBytes := entryBytes[:29]

				// use the information to determine wheter we have enough bytes to form whole entry
				// if thats not the case -> read another file and pray :>
				keySize := binary.LittleEndian.Uint64(headerBytes[13:21])
				valueSize := binary.LittleEndian.Uint64(headerBytes[21:29])

				totalSize := int(29 + keySize + valueSize)

				if len(entryBytes) >= totalSize {
					print("totalSize: ", totalSize)
					// form an entry
					walEntry := WalEntryFromBytes(entryBytes[:totalSize])
					// fmt.Println(walEntry.Validate())
					full, sizeToDelete := table.Add(memTable.NewMemTableEntry(string(walEntry.Key), walEntry.Value, walEntry.Tombstone, walEntry.Timestamp))
					if full != nil {
						if config.GlobalConfig.SStableAllInOne == false {
							if config.GlobalConfig.SStableDegree != 0 {
								sstable.CreateSStable_13(full, 1, config.GlobalConfig.SStableDegree)
							} else {
								sstable.CreateSStable(full, 1)
							}
						} else {
							sstable.NewSSTable(&full, 1)
						}
						fmt.Println(sizeToDelete) // Ovde treba pozvati brisanje wal segmenata
						// deleteSegmentsVarijantaKojaBrise toliko
					}

					// move both to the next entry
					entryBytes = entryBytes[totalSize:]
					partialEntry = partialEntry[totalSize:]
				} else {
					// save the partial entry for the next iteration
					partialEntry = entryBytes
					break
				}
			}
		}
	}
}
