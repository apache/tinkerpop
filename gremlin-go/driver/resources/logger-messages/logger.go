// Code generated for package bindata_error by go-bindata DO NOT EDIT. (@generated)
// sources:
// resources/logger-messages/en.json
package bindata_error

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func bindataRead(data []byte, name string) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return nil, fmt.Errorf("Read %q: %v", name, err)
	}

	var buf bytes.Buffer
	_, err = io.Copy(&buf, gz)
	clErr := gz.Close()

	if err != nil {
		return nil, fmt.Errorf("Read %q: %v", name, err)
	}
	if clErr != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

type asset struct {
	bytes []byte
	info  os.FileInfo
}

type bindataFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
}

// Name return file name
func (fi bindataFileInfo) Name() string {
	return fi.name
}

// Size return file size
func (fi bindataFileInfo) Size() int64 {
	return fi.size
}

// Mode return file mode
func (fi bindataFileInfo) Mode() os.FileMode {
	return fi.mode
}

// Mode return file modify time
func (fi bindataFileInfo) ModTime() time.Time {
	return fi.modTime
}

// IsDir return file whether a directory
func (fi bindataFileInfo) IsDir() bool {
	return fi.mode&os.ModeDir != 0
}

// Sys return file is sys mode
func (fi bindataFileInfo) Sys() interface{} {
	return nil
}

var _enJson = []byte("\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff\x94\x95\x41\x6f\xe3\x36\x13\x86\xef\xf9\x15\x03\x03\xc1\x1e\xbe\xdd\x00\xdf\x35\x45\x0f\x8a\xc4\x04\xc2\xca\x92\x2b\xc9\x9b\x2c\x50\x40\xa0\xc5\x89\x4d\x84\x22\x55\x92\x8a\xb3\x2d\xfa\xdf\x0b\x52\xb4\x25\x27\x4e\x37\x3d\x5a\xe6\x3c\x33\xc3\x79\x5f\xce\x5f\x17\x00\x8b\x75\xfe\x35\x2f\xee\xf3\xa6\x22\x65\x93\x44\x75\x54\x7f\x5f\x91\xc5\x35\x2c\xd6\xf2\x49\xaa\xbd\x04\x46\x2d\x05\xfb\xa3\x47\xb0\x0a\x0c\x6a\x4e\x05\xff\x13\xfd\x97\x5f\x3f\x5d\x9a\x4f\x57\x8b\xcf\x73\x4c\x42\x3e\x04\x62\x38\xa1\x5a\xc5\x1c\xea\xe5\x80\xca\xd7\x59\xd6\xa4\xf9\x6a\x5d\xbb\xf0\x54\xf6\x83\x85\x96\x4a\xa9\x2c\x6c\x10\xe4\x20\xc4\x31\x27\x79\x58\x91\xb8\x26\x49\xe3\x63\xbe\x45\xd9\x3a\xa4\xc4\x97\x1e\x5b\x8b\xcc\x1f\x87\x67\x2a\x06\x9f\x76\xaf\xb9\x45\xd8\xef\x50\x86\x6f\xdc\x80\xe3\xba\x53\x74\x23\x30\x80\xe3\xac\xa8\xd2\xfc\xae\x89\x8b\x3c\x27\x71\x9d\x16\xb9\xa3\xc6\x42\x19\x2e\xb7\x60\x77\xae\x66\x29\xb1\xb5\x5c\xc9\x10\x72\x1b\xa5\x19\x49\xe6\x11\x70\x0d\x8b\x5b\xca\x05\x32\x97\x99\x4b\x63\xa9\xb4\x9c\x5a\xf4\x00\x89\xfb\x19\xe4\x17\x30\x68\xad\x83\x4f\xdf\xc0\x58\x7f\x58\x41\x2b\x94\x41\x16\x12\x15\x2b\x92\x9f\xa9\x2d\x84\xc9\x6d\x38\x76\x5f\xa6\x35\x69\x4a\xf2\xdb\x9a\x54\xfe\x1e\xef\x35\xf7\x09\x34\xfe\x31\xa0\xb1\xe1\x58\x49\xa2\xa4\xc9\x8a\x62\xd5\x90\xb2\x2c\x4a\x77\xb0\x44\xca\x40\x28\xd5\x03\x6a\xad\x34\xb8\x21\x7f\xf6\x35\x8c\xe1\xe1\xdf\x00\xf0\x61\x4d\x1c\x65\xd9\x4d\x14\x7f\x9d\x97\xa2\x64\x00\xb4\x54\x88\x0d\x6d\x9f\x80\xcb\x67\xf5\x84\x6c\x82\xf5\x5a\x59\xd5\xaa\xc3\x3c\xe3\x92\x44\xb5\xeb\x6d\x56\x76\xac\x91\xba\x39\x86\xb2\x61\xcf\xed\x0e\x38\xf3\x55\xcd\x5a\x88\x8b\xe5\x2a\x23\x35\x39\x36\xd0\xaa\xae\x17\x68\x11\x1e\x95\x06\x8d\xa6\x57\xd2\xe0\x99\xe8\x6a\x7d\xb3\x4c\xeb\xa6\xaa\xa3\xd2\xe9\xa8\xaa\xcb\x34\xbf\x1b\xa7\xcd\x51\xda\x43\xcd\x50\x0d\x9b\x8e\x87\xf4\xc6\x6a\x57\x3d\xd5\xdb\xa1\x43\x69\xaf\xe1\xd2\x9c\x85\xdd\x7c\xaf\x49\x5c\x24\xe4\x27\xb8\xcd\x0f\x8b\xce\x03\x73\xe0\xff\x9e\x7f\x97\x27\xc2\xaa\x8b\xc6\xa9\x92\x34\x69\xde\xbc\xbd\xf3\x49\x67\x5e\x2b\xc7\x8b\x05\xfe\x66\x08\x53\xeb\x13\x79\x14\xcb\x92\x54\x55\x74\x47\x4e\x79\xa3\x63\x3a\x34\x86\x6e\x47\xd7\x5f\x07\x45\x78\xee\xf5\x59\x5e\x45\xea\xc0\x4c\x48\x94\x64\x69\xfe\x0a\x6a\xd0\x06\x30\x43\xca\x04\x97\x78\x06\x97\x15\x77\xa1\xd3\x3b\x92\x93\x32\x8d\x1d\x83\xf8\x66\x54\xdb\x0e\x5a\x23\x03\x36\xf8\x49\xa8\x1e\x35\xf5\x8a\xbb\x34\x73\xc4\x51\x50\x15\xa9\xaa\xb4\xc8\x5f\x9b\xc6\x69\xcb\xc5\x57\x68\x8c\x8b\xde\x50\x83\x6c\x66\xc1\xd3\xd7\xa0\x5a\x45\xf7\xb9\xd3\xc8\x08\xab\xe6\x6f\x82\xe9\xe9\x5e\x22\x03\x33\x92\x0c\x3c\x6a\xd5\x41\xa2\xf9\x33\xea\x12\x3b\x65\x71\x66\x0b\x3f\xf5\x41\x8b\x79\xa5\x7e\xb6\x81\x3c\x07\x7f\x04\x11\x54\x19\x9a\x78\x07\x7a\x30\xd5\x64\xf4\xf1\x2e\xf7\x3b\x2e\xf0\xe8\xc8\x0f\xa5\xa3\x92\xbd\x4d\x39\xb9\x60\x4c\x9b\x94\xe9\x37\x52\x36\x25\x59\x16\x35\x79\xe7\x25\xfd\xaf\xf7\x13\x67\x29\xc9\xeb\x39\x21\xd8\xea\x4c\x44\x30\xc9\xd9\x97\x3c\xdd\x4a\xe5\x85\x13\xbc\x11\x60\xd3\xe0\x67\xcd\x38\x09\xcd\xeb\x7f\x7d\x7f\xed\x41\x44\xa7\x2f\xba\x7f\x77\x66\x3f\x7b\xa5\xc4\x44\x5d\x15\x45\xd6\xe4\xe4\xfe\x2c\xf7\x96\x0a\xe1\x80\xde\xae\x56\x81\x40\x6a\xec\x97\xe1\x54\x9a\x57\x10\xbf\x93\x98\x8d\xab\xee\x6c\x14\xe0\x4b\x8b\xc8\x42\xb3\xde\x44\xd2\xc2\x30\x7a\x7b\xa7\xd1\xec\x94\x60\xf0\xe8\x8d\x3a\x7b\xd6\x82\x82\x12\x52\xfb\x6d\xeb\x8a\x3c\x58\x86\xa1\xf5\x8b\xf6\x0a\xaa\xb7\x1b\xcc\x35\x0d\xc6\xed\xf8\x8e\xbe\xf0\x6e\xe8\x5c\x61\xff\xbf\x9a\xdd\x41\x9a\xa7\x75\x1a\x65\x0d\x79\x88\x09\x49\xaa\x66\x19\x3d\xa4\xcb\xf5\x72\x5c\xfc\xdc\x72\x2a\xe2\x63\x9d\x93\x42\xcc\x71\x5d\x5e\xb2\xd0\x12\x32\x58\x8e\x39\x7e\x1a\xf0\x05\x04\xef\xc6\x65\xf8\xaf\x39\xac\x82\x4b\x76\xb5\xb8\xf8\xfb\xe2\x9f\x00\x00\x00\xff\xff\xcc\x77\x76\x51\x29\x09\x00\x00")

func enJsonBytes() ([]byte, error) {
	return bindataRead(
		_enJson,
		"en.json",
	)
}

func enJson() (*asset, error) {
	bytes, err := enJsonBytes()
	if err != nil {
		return nil, err
	}

	info := bindataFileInfo{name: "en.json", size: 2345, mode: os.FileMode(420), modTime: time.Unix(1665760938, 0)}
	a := &asset{bytes: bytes, info: info}
	return a, nil
}

// Asset loads and returns the asset for the given name.
// It returns an error if the asset could not be found or
// could not be loaded.
func Asset(name string) ([]byte, error) {
	cannonicalName := strings.Replace(name, "\\", "/", -1)
	if f, ok := _bindata[cannonicalName]; ok {
		a, err := f()
		if err != nil {
			return nil, fmt.Errorf("Asset %s can't read by error: %v", name, err)
		}
		return a.bytes, nil
	}
	return nil, fmt.Errorf("Asset %s not found", name)
}

// MustAsset is like Asset but panics when Asset would return an error.
// It simplifies safe initialization of global variables.
func MustAsset(name string) []byte {
	a, err := Asset(name)
	if err != nil {
		panic("asset: Asset(" + name + "): " + err.Error())
	}

	return a
}

// AssetInfo loads and returns the asset info for the given name.
// It returns an error if the asset could not be found or
// could not be loaded.
func AssetInfo(name string) (os.FileInfo, error) {
	cannonicalName := strings.Replace(name, "\\", "/", -1)
	if f, ok := _bindata[cannonicalName]; ok {
		a, err := f()
		if err != nil {
			return nil, fmt.Errorf("AssetInfo %s can't read by error: %v", name, err)
		}
		return a.info, nil
	}
	return nil, fmt.Errorf("AssetInfo %s not found", name)
}

// AssetNames returns the names of the assets.
func AssetNames() []string {
	names := make([]string, 0, len(_bindata))
	for name := range _bindata {
		names = append(names, name)
	}
	return names
}

// _bindata is a table, holding each asset generator, mapped to its name.
var _bindata = map[string]func() (*asset, error){
	"en.json": enJson,
}

// AssetDir returns the file names below a certain
// directory embedded in the file by go-bindata.
// For example if you run go-bindata on data/... and data contains the
// following hierarchy:
//     data/
//       foo.txt
//       img/
//         a.png
//         b.png
// then AssetDir("data") would return []string{"foo.txt", "img"}
// AssetDir("data/img") would return []string{"a.png", "b.png"}
// AssetDir("foo.txt") and AssetDir("notexist") would return an error
// AssetDir("") will return []string{"data"}.
func AssetDir(name string) ([]string, error) {
	node := _bintree
	if len(name) != 0 {
		cannonicalName := strings.Replace(name, "\\", "/", -1)
		pathList := strings.Split(cannonicalName, "/")
		for _, p := range pathList {
			node = node.Children[p]
			if node == nil {
				return nil, fmt.Errorf("Asset %s not found", name)
			}
		}
	}
	if node.Func != nil {
		return nil, fmt.Errorf("Asset %s not found", name)
	}
	rv := make([]string, 0, len(node.Children))
	for childName := range node.Children {
		rv = append(rv, childName)
	}
	return rv, nil
}

type bintree struct {
	Func     func() (*asset, error)
	Children map[string]*bintree
}

var _bintree = &bintree{nil, map[string]*bintree{
	"en.json": &bintree{enJson, map[string]*bintree{}},
}}

// RestoreAsset restores an asset under the given directory
func RestoreAsset(dir, name string) error {
	data, err := Asset(name)
	if err != nil {
		return err
	}
	info, err := AssetInfo(name)
	if err != nil {
		return err
	}
	err = os.MkdirAll(_filePath(dir, filepath.Dir(name)), os.FileMode(0755))
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(_filePath(dir, name), data, info.Mode())
	if err != nil {
		return err
	}
	err = os.Chtimes(_filePath(dir, name), info.ModTime(), info.ModTime())
	if err != nil {
		return err
	}
	return nil
}

// RestoreAssets restores an asset under the given directory recursively
func RestoreAssets(dir, name string) error {
	children, err := AssetDir(name)
	// File
	if err != nil {
		return RestoreAsset(dir, name)
	}
	// Dir
	for _, child := range children {
		err = RestoreAssets(dir, filepath.Join(name, child))
		if err != nil {
			return err
		}
	}
	return nil
}

func _filePath(dir, name string) string {
	cannonicalName := strings.Replace(name, "\\", "/", -1)
	return filepath.Join(append([]string{dir}, strings.Split(cannonicalName, "/")...)...)
}
