package main

import (
	"encoding/gob"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type File struct {
	Id    uint64
	Paths []Path
	Size  int64
}

type Path struct {
	Base string
	Dir  string
	Name string
}

func (self Path) Equal(p Path) bool {
	return self.Base+self.Dir+self.Name == p.Base+p.Dir+p.Name
}

type Database struct {
	Files map[uint64]*File
	path  string
}

func NewDatabase(filePath string) *Database {
	db := &Database{
		Files: make(map[uint64]*File),
		path:  filePath,
	}
	_, err := os.Stat(filePath)
	if err == nil { // db file exists
		dbFile, err := os.Open(filePath)
		if err != nil {
			log.Fatal(err)
		}
		err = gob.NewDecoder(dbFile).Decode(db)
		if err != nil {
			log.Fatal(err)
		}
	}
	return db
}

func (self *Database) ImportDir(dir string, base string) error {
	var err error
	dir, err = filepath.Abs(dir)
	if err != nil {
		return err
	}
	base, err = filepath.Abs(base)
	if err != nil {
		return err
	}
	dirInfo, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !dirInfo.IsDir() {
		return errors.New(fmt.Sprintf("%s is not a directory", dir))
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer dirFile.Close()
	subInfos, err := dirFile.Readdir(0)
	if err != nil {
		return err
	}
	hasher := fnv.New64()
	for _, info := range subInfos {
		if info.IsDir() { // sub dir
			err = self.ImportDir(filepath.Join(dir, info.Name()), base)
			if err != nil {
				return err
			}
		} else { // file
			filePath := filepath.Join(dir, info.Name())
			// file info
			fileFile, err := os.Open(filePath)
			if err != nil {
				return err
			}
			defer fileFile.Close()
			hasher.Reset()
			io.Copy(hasher, fileFile)
			sum := hasher.Sum64()
			file, exists := self.Files[sum]
			if exists {
				if file.Size != info.Size() { // conflict hash
					return errors.New(fmt.Sprintf("conflict hash: %v %v", info, file))
				}
			} else {
				self.Files[sum] = &File{
					Id:    sum,
					Paths: make([]Path, 0),
					Size:  info.Size(),
				}
			}
			file = self.Files[sum]
			// path info
			relPath := strings.TrimPrefix(filePath, base)
			fileDir, fileName := filepath.Split(relPath)
			exists = false
			path := Path{
				Base: base,
				Dir:  fileDir,
				Name: fileName,
			}
			for _, p := range file.Paths {
				if p.Equal(path) {
					exists = true
					break
				}
			}
			if !exists {
				file.Paths = append(file.Paths, path)
				fmt.Printf("%v\n", file)
			} else {
				fmt.Printf("skip %v\n", path)
			}
		}
	}
	self.Save() //TODO safer
	return nil
}

func (self *Database) Save() {
	tmpPath := fmt.Sprintf("%s-%d", self.path, rand.Int31())
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		log.Fatal(err)
	}
	err = gob.NewEncoder(tmpFile).Encode(self)
	if err != nil {
		log.Fatal(err)
	}
	tmpFile.Close()
	os.Rename(tmpPath, self.path)
}

func main() {
	db := NewDatabase("/home/reus/.songcollect")
	err := db.ImportDir("/media/C/Musics", "/media/C/Musics")
	if err != nil {
		log.Fatal(err)
	}
}
