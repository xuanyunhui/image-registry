// Package cos provides a storagedriver.StorageDriver implementation to
// store blobs in Qcloud cos cloud storage.
//

package cos

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"
	"context"

	cos "cosapi"

	"github.com/sirupsen/logrus"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

const driverName = "cos"

// minChunkSize defines the minimum multipart upload chunk size
// COS API requires multipart upload chunks to be at least 1MB
const minChunkSize = 1 << 20

const defaultChunkSize = 2 * minChunkSize

// const defaultTimeout = 2 * time.Minute // 2 minute timeout per chunk

// listMax is the largest amount of objects you can request from COS in a list call
const listMax = 1000

//DriverParameters A struct that encapsulates all of the driver parameters after all values have been set
type DriverParameters struct {
	AccessKeyID     string
	AccessKeySecret string
	AppID           string
	Bucket          string
	Region          cos.Region
	Secure          bool
	Debug           bool
	ChunkSize       int64
	RootDirectory   string
	Endpoint        string
}

func init() {
	factory.Register(driverName, &cosDriverFactory{})
}

// cosDriverFactory implements the factory.StorageDriverFactory interface
type cosDriverFactory struct{}

func (factory *cosDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type driver struct {
	Client        *cos.Client
	Bucket        *cos.Bucket
	ChunkSize     int64
	RootDirectory string
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by Qcloud COS
// Objects are stored at absolute keys in the provided bucket.
type Driver struct {
	baseEmbed
}

// FromParameters constructs a new Driver with a given parameters map
// Required parameters:
// - accesskey
// - secretkey
// - appid
// - region
// - bucket
// - encrypt
func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	// Providing no values for these is valid in case the user is authenticating

	var regionName, bucket, appId string

	accessKey, ok := parameters["secretid"]
	if !ok {
		return nil, fmt.Errorf("No secretid parameter provided")
	}
	secretKey, ok := parameters["secretkey"]
	if !ok {
		return nil, fmt.Errorf("No secretkey parameter provided")
	}

	endpoint, ok := parameters["endpoint"]
	if !ok {
		endpoint = ""
		regionName, ok = parameters["region"].(string)
		if !ok || regionName == "" {
			return nil, fmt.Errorf("No region parameter provided")
		}

		bucket, ok = parameters["bucket"].(string)
		if !ok || bucket == "" {
			return nil, fmt.Errorf("No bucket parameter provided")
		}

		appIdVal, ok := parameters["appid"]
		if ok {
			switch val := appIdVal.(type) {
			case string:
				appId = val
			default:
				appId = fmt.Sprint(val)
			}
		} else {
			return nil, fmt.Errorf("No appid parameter provided")
		}
	}

	secureBool := false
	secure, ok := parameters["secure"]
	if ok {
		secureBool, ok = secure.(bool)
		if !ok {
			return nil, fmt.Errorf("The secure parameter should be a boolean")
		}
	}

	debugBool := false
	debug, ok := parameters["debug"]
	if ok {
		debugBool, ok = debug.(bool)
		if !ok {
			return nil, fmt.Errorf("The debug parameter should be a boolean")
		}
	}

	chunkSize := int64(defaultChunkSize)
	chunkSizeParam, ok := parameters["chunksize"]
	if ok {
		switch v := chunkSizeParam.(type) {
		case string:
			vv, err := strconv.ParseInt(v, 0, 64)
			if err != nil {
				return nil, fmt.Errorf("chunksize parameter must be an integer, %v invalid", chunkSizeParam)
			}
			chunkSize = vv
		case int64:
			chunkSize = v
		case int, uint, int32, uint32, uint64:
			chunkSize = reflect.ValueOf(v).Convert(reflect.TypeOf(chunkSize)).Int()
		default:
			return nil, fmt.Errorf("invalid valud for chunksize: %#v", chunkSizeParam)
		}

		if chunkSize < minChunkSize {
			return nil, fmt.Errorf("The chunksize %#v parameter should be a number that is larger than or equal to %d", chunkSize, minChunkSize)
		}
	}

	rootDirectory, ok := parameters["rootdirectory"]
	if !ok {
		rootDirectory = ""
	}

	params := DriverParameters{
		AccessKeyID:     fmt.Sprint(accessKey),
		AccessKeySecret: fmt.Sprint(secretKey),
		AppID:           appId,
		Bucket:          bucket,
		Region:          cos.Region(regionName),
		ChunkSize:       chunkSize,
		RootDirectory:   fmt.Sprint(rootDirectory),
		Secure:          secureBool,
		Debug:           debugBool,
		Endpoint:        fmt.Sprint(endpoint),
	}

	return New(params)
}

// New constructs a new Driver with the given Qcloud credentials, region, encryption flag, and
// bucketName
func New(params DriverParameters) (*Driver, error) {

	client := cos.NewCOSClient(params.Region, params.AppID, params.AccessKeyID, params.AccessKeySecret, params.Secure, params.Debug)
	client.SetEndpoint(params.Endpoint)
	bucket := client.Bucket(params.Bucket)
	//client.SetDebug(false)

	// Validate that the given credentials have at least read permissions in the
	// given bucket scope.
	if _, err := bucket.List(strings.TrimRight(params.RootDirectory, "/"), "", "", 1); err != nil {
		return nil, err
	}

	// TODO(tg123): Currently multipart uploads have no timestamps, so this would be unwise
	// if you initiated a new COS client while another one is running on the same bucket.

	d := &driver{
		Client:        client,
		Bucket:        bucket,
		ChunkSize:     params.ChunkSize,
		RootDirectory: params.RootDirectory,
	}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

func isPathConflictError(err error) bool {
	if err != nil {
		if cosErr, ok := err.(*cos.Error); ok {
			if cosErr.StatusCode == 409 && cosErr.Code == "PathConflict" {
				return true
			}
		}
	}
	return false
}

func retryPathConflict(label string, path string, action func() error) error {
	var err error
	const repeats = 3
	for i := 1; i <= repeats; i += 1 {
		err = action()
		if isPathConflictError(err) {
			time.Sleep(time.Duration(500*i) * time.Millisecond)
			logrus.Warnf("%s retry. times=%d, path=%s", label, i, path)
			continue
		}
		break
	}

	if isPathConflictError(err) {
		logrus.Errorf("%s Failed. path=%s, err=%+v", label, path, err)
	}

	return err
}

// Implement the storagedriver.StorageDriver interface

func (d *driver) Name() string {
	if d.Client.Debug {
		log.Printf("driver Name:%s", driverName)
	}
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	if d.Client.Debug {
		log.Printf("driver GetContent, path:%s", path)
	}
	content, err := d.Bucket.Get(d.cosPath(path))
	if err != nil {
		return nil, parseError(path, err)
	}
	return content, nil
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	var action = func() error {
		return d.Bucket.Put(d.cosPath(path), contents, d.getContentType(), getPermissions(), d.getOptions())
	}
	var err = retryPathConflict("PutContent", path, action)
	return parseError(path, err)
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	headers := make(http.Header)
	headers.Add("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")

	resp, err := d.Bucket.GetResponseWithHeaders(d.cosPath(path), headers)
	if err != nil {
		return nil, parseError(path, err)
	}

	// Due to Qcloud COS API, status 200 and whole object will be return instead of an
	// InvalidRange error when range is invalid.
	//
	// COS sever will always return http.StatusPartialContent if range is acceptable.
	if resp.StatusCode != http.StatusPartialContent {
		resp.Body.Close()
		return ioutil.NopCloser(bytes.NewReader(nil)), nil
	}

	return resp.Body, nil
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file
func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {
	return storagedriver.WalkFallback(ctx, d, path, f)
}
// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	key := d.cosPath(path)
	if !append {
		// TODO (brianbland): cancel other uploads at this path
		multi, err := d.Bucket.InitMulti(key, d.getContentType(), getPermissions(), d.getOptions())
		if err != nil {
			return nil, err
		}
		return d.newWriter(key, multi, nil), nil
	}
	multis, _, err := d.Bucket.ListMulti(key, "")
	if err != nil {
		return nil, parseError(path, err)
	}
	for _, multi := range multis {
		if key != multi.Key {
			continue
		}
		parts, err := multi.ListParts()
		if err != nil {
			return nil, parseError(path, err)
		}

		return d.newWriter(key, multi, parts), nil
	}
	return nil, storagedriver.PathNotFoundError{Path: path}
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	listResponse, err := d.Bucket.List(d.cosPath(path), "", "", 1)
	if err != nil {
		return nil, err
	}

	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	if len(listResponse.Contents) == 1 {
		if listResponse.Contents[0].Key != d.cosPath(path) {
			fi.IsDir = true
		} else {
			fi.IsDir = false
			fi.Size = listResponse.Contents[0].Size

			timestamp, err := time.Parse(time.RFC3339Nano, listResponse.Contents[0].LastModified)
			if err != nil {
				return nil, err
			}
			fi.ModTime = timestamp
		}
	} else if len(listResponse.CommonPrefixes) == 1 {
		fi.IsDir = true
	} else {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the given path.
func (d *driver) List(ctx context.Context, opath string) ([]string, error) {
	path := opath
	if path != "/" && opath[len(path)-1] != '/' {
		path = path + "/"
	}

	// This is to cover for the cases when the rootDirectory of the driver is either "" or "/".
	// In those cases, there is no root prefix to replace and we must actually add a "/" to all
	// results in order to keep them as valid paths as recognized by storagedriver.PathRegexp
	prefix := ""
	if d.cosPath("") == "" {
		prefix = "/"
	}

	listResponse, err := d.Bucket.List(d.cosPath(path), "/", "", listMax)
	if err != nil {
		return nil, parseError(opath, err)
	}

	files := []string{}
	directories := []string{}

	for {
		for _, key := range listResponse.Contents {
			files = append(files, strings.Replace(key.Key, d.cosPath(""), prefix, 1))
		}

		for _, commonPrefix := range listResponse.CommonPrefixes {
			directories = append(directories, strings.Replace(commonPrefix[0:len(commonPrefix)-1], d.cosPath(""), prefix, 1))
		}

		if listResponse.IsTruncated {
			listResponse, err = d.Bucket.List(d.cosPath(path), "/", listResponse.NextMarker, listMax)
			if err != nil {
				return nil, err
			}
		} else {
			break
		}
	}

	if opath != "/" {
		if len(files) == 0 && len(directories) == 0 {
			return nil, storagedriver.PathNotFoundError{Path: opath}
		}
	}

	return append(files, directories...), nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	logrus.Infof("Move from %s to %s", d.cosPath(sourcePath), d.cosPath(destPath))

	var action = func() error {
		/* This is terrible, but aws doesn't have an actual move. */
		_, err := d.Bucket.PutCopy(d.cosPath(destPath), getPermissions(),
			cos.CopyOptions{}, d.Client.GetHost(d.Bucket.Name)+"/"+d.cosPath(sourcePath))
		return err
	}

	var err = retryPathConflict("Move", destPath, action)
	if err != nil {
		return parseError(sourcePath, err)
	}

	return d.Delete(ctx, sourcePath)
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {
	listResponse, err := d.Bucket.List(d.cosPath(path), "", "", listMax)
	if err != nil || len(listResponse.Contents) == 0 {
		return storagedriver.PathNotFoundError{Path: path}
	}

	cosObjects := make([]cos.Object, listMax)

	for len(listResponse.Contents) > 0 {
		for index, key := range listResponse.Contents {
			cosObjects[index].Key = key.Key
		}

		err := d.Bucket.DelMulti(cos.Delete{Quiet: false, Objects: cosObjects[0:len(listResponse.Contents)]})
		if err != nil {
			return nil
		}

		listResponse, err = d.Bucket.List(d.cosPath(path), "", "", listMax)
		if err != nil {
			return err
		}
	}

	return nil
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	methodString := "GET"
	method, ok := options["method"]
	if ok {
		methodString, ok = method.(string)
		if !ok || (methodString != "GET") {
			return "", storagedriver.ErrUnsupportedMethod{}
		}
	}

	expiresTime := time.Now().Add(20 * time.Minute)

	expires, ok := options["expiry"]
	if ok {
		et, ok := expires.(time.Time)
		if ok {
			expiresTime = et
		}
	}
	logrus.Infof("methodString: %s, expiresTime: %v", methodString, expiresTime)
	signedURL := d.Bucket.SignedURLWithMethod(methodString, d.cosPath(path), expiresTime, nil, nil)
	logrus.Infof("signed URL: %s", signedURL)
	return signedURL, nil
}

func (d *driver) cosPath(path string) string {
	return strings.TrimLeft(strings.TrimRight(d.RootDirectory, "/")+path, "/")
}

func parseError(path string, err error) error {
	if cosErr, ok := err.(*cos.Error); ok && cosErr.StatusCode == http.StatusNotFound {
		return storagedriver.PathNotFoundError{Path: path}
	}
	return err
}

func (d *driver) getOptions() cos.Options {
	return cos.Options{}
}

func getPermissions() cos.ACL {
	return cos.Private
}

func (d *driver) getContentType() string {
	return "application/octet-stream"
}

// writer attempts to upload parts to COS in a buffered fashion where the last
// part is at least as large as the chunksize, so the multipart upload could be
// cleanly resumed in the future. This is violated if Close is called after less
// than a full chunk is written.
type writer struct {
	driver      *driver
	key         string
	multi       *cos.Multi
	parts       []cos.Part
	size        int64
	readyPart   []byte
	pendingPart []byte
	closed      bool
	committed   bool
	cancelled   bool
}

func (d *driver) newWriter(key string, multi *cos.Multi, parts []cos.Part) storagedriver.FileWriter {
	var size int64
	for _, part := range parts {
		size += part.Size
	}
	return &writer{
		driver: d,
		key:    key,
		multi:  multi,
		parts:  parts,
		size:   size,
	}
}

func (w *writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("already closed")
	} else if w.committed {
		return 0, fmt.Errorf("already committed")
	} else if w.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}

	// If the last written part is smaller than minChunkSize, we need to make a
	// new multipart upload :sadface:
	if len(w.parts) > 0 && int(w.parts[len(w.parts)-1].Size) < minChunkSize {
		err := w.multi.Complete(w.parts)
		if err != nil {
			w.multi.Abort()
			return 0, err
		}

		multi, err := w.driver.Bucket.InitMulti(w.key, w.driver.getContentType(), getPermissions(), w.driver.getOptions())
		if err != nil {
			return 0, err
		}
		w.multi = multi

		// If the entire written file is smaller than minChunkSize, we need to make
		// a new part from scratch :double sad face:
		if w.size < minChunkSize {
			contents, err := w.driver.Bucket.Get(w.key)
			if err != nil {
				return 0, err
			}
			w.parts = nil
			w.readyPart = contents
		} else {
			/***************************
			// Otherwise we can use the old file as the new first part
			_, part, err := multi.PutPartCopy(1, cos.CopyOptions{}, w.driver.Bucket.Name+"/"+w.key)
			if err != nil {
				return 0, err
			}
			w.parts = []cos.Part{part}
			****************************/

			logrus.Infof("writer info, size: %d, key: %s, minChunkSize:%d", w.size, w.key, minChunkSize)
			return 0, fmt.Errorf("not support")
		}
	}

	var n int

	for len(p) > 0 {
		// If no parts are ready to write, fill up the first part
		if neededBytes := int(w.driver.ChunkSize) - len(w.readyPart); neededBytes > 0 {
			if len(p) >= neededBytes {
				w.readyPart = append(w.readyPart, p[:neededBytes]...)
				n += neededBytes
				p = p[neededBytes:]
			} else {
				w.readyPart = append(w.readyPart, p...)
				n += len(p)
				p = nil
			}
		}

		if neededBytes := int(w.driver.ChunkSize) - len(w.pendingPart); neededBytes > 0 {
			if len(p) >= neededBytes {
				w.pendingPart = append(w.pendingPart, p[:neededBytes]...)
				n += neededBytes
				p = p[neededBytes:]
				err := w.flushPart()
				if err != nil {
					w.size += int64(n)
					return n, err
				}
			} else {
				w.pendingPart = append(w.pendingPart, p...)
				n += len(p)
				p = nil
			}
		}
	}
	w.size += int64(n)
	return n, nil
}

func (w *writer) Size() int64 {
	return w.size
}

func (w *writer) Close() error {
	if w.closed {
		return fmt.Errorf("already closed")
	}
	w.closed = true
	return w.flushPart()
}

func (w *writer) Cancel() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	}
	w.cancelled = true
	err := w.multi.Abort()
	return err
}

func (w *writer) Commit() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	} else if w.cancelled {
		return fmt.Errorf("already cancelled")
	}
	err := w.flushPart()
	if err != nil {
		return err
	}
	w.committed = true
	err = w.multi.Complete(w.parts)
	if err != nil {
		w.multi.Abort()
		return err
	}
	return nil
}

// flushPart flushes buffers to write a part to COS.
// Only called by Write (with both buffers full) and Close/Commit (always)
func (w *writer) flushPart() error {
	if len(w.readyPart) == 0 && len(w.pendingPart) == 0 {
		// nothing to write
		return nil
	}
	if len(w.pendingPart) < int(w.driver.ChunkSize) {
		// closing with a small pending part
		// combine ready and pending to avoid writing a small part
		w.readyPart = append(w.readyPart, w.pendingPart...)
		w.pendingPart = nil
	}

	part, err := w.multi.PutPart(len(w.parts)+1, bytes.NewReader(w.readyPart))
	if err != nil {
		return err
	}
	w.parts = append(w.parts, part)
	w.readyPart = w.pendingPart
	w.pendingPart = nil
	return nil
}
