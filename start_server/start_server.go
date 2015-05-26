package main

import (
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "io/ioutil"
    "kvmap"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "sync"
)

const (
    ERROR_MODEL = iota
    PRIMARY_MODEL
    BACKUP_MODEL
)
var model int
var primary_ip, backup_ip, port string
var data = kvmap.NewKVmap()
var mutex sync.Mutex
var crashed bool

// Analyse the command-line arguments and get the running model
func loadModel() {
    if (len(os.Args) == 1) {
        model = PRIMARY_MODEL
    } else if (len(os.Args) > 2) {
        model = ERROR_MODEL
    } else if (os.Args[1] == "-p") {
        model = PRIMARY_MODEL
    } else if (os.Args[1] == "-b") {
        model = BACKUP_MODEL
    } else {
        model = ERROR_MODEL
    }
}

// Load configurations from "conf/settings.conf"
// Get primary ip, backup ip and port
func loadConfig() error {
    fin, err := os.Open("../conf/settings.conf")
    defer fin.Close()
    if err != nil {
        return err
    }

    buf := make([]byte, 4096)
    n, err := fin.Read(buf)
    if err != nil {
        return err
    }
    if n == 0 {
        return errors.New("the config file is empty")
    }

    var config map[string]string
    err = json.Unmarshal(buf[:n], &config)
    if err != nil {
        return err
    }

    var exist bool
    primary_ip, exist = config["primary"]
    if !exist {
        return errors.New("config file error")
    }
    backup_ip, exist = config["backup"]
    if !exist {
        return errors.New("config file error")
    }
    port, exist = config["port"]
    port = ":" + port
    if !exist {
        return errors.New("config file error")
    }

    return nil
}

// Get data back from the backup server
func getDataFromBackup() {
    resp, err := http.Get("http://" + backup_ip + port + "/kvman/dump")
    if err != nil {
        return
    }

    defer resp.Body.Close()
    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return
    }

    mutex.Lock()
    defer mutex.Unlock()
    data.Unserialize(body)
}

// Start server
func startServer() {
    mux := http.NewServeMux()
    mux.HandleFunc("/", handle_root)
    mux.HandleFunc("/kv/insert", handle_kv_insert)
    mux.HandleFunc("/kv/delete", handle_kv_delete)
    mux.HandleFunc("/kv/get", handle_kv_get)
    mux.HandleFunc("/kv/update", handle_kv_update)
    mux.HandleFunc("/kvman/countkey", handle_kvman_countkey)
    mux.HandleFunc("/kvman/dump", handle_kvman_dump)
    mux.HandleFunc("/kvman/shutdown", handle_kvman_shutdown)
    mux.HandleFunc("/kvman/refresh", handle_kvman_refresh)
    http.ListenAndServe(port, mux)
}

func main() {
    loadModel()
    if model == ERROR_MODEL {
        fmt.Println("Error: illegal command-line arguments.")
        return
    }

    err := loadConfig()
    if err != nil {
        fmt.Println("Error:", err)
        return
    }

    if model == PRIMARY_MODEL {
        crashed = true
        getDataFromBackup()
    }

    startServer()
}


// Handlers functions are the followings

func askBackupToRefresh() bool {
    resp, err := http.PostForm("http://" + backup_ip + port + "/kvman/refresh", url.Values{"value":{data.ToString()}})
    if err != nil {
        return false
    }

    defer resp.Body.Close()
    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return false
    }

    if string(body) != "true" {
        return false
    }

    return true
}

// Root: a convenient way to test our code using web browser
func handle_root(wfile http.ResponseWriter, request *http.Request) {
    io.WriteString(wfile, "<html>\n<body>\n" +
                          "<p>Insert a (key, value) pair.</p>\n" +
                          "<form action=\"/kv/insert\" method=\"post\">\n" +
                          "  <p>Key: <input type=\"text\" name=\"key\" /></p>\n" +
                          "  <p>Value: <input type=\"text\" name=\"value\" /></p>\n" +
                          "  <input type=\"submit\" value=\"Submit\" />\n" +
                          "</form>\n" +
                          "<p>Delete a key.</p>\n" +
                          "<form action=\"/kv/delete\" method=\"post\">\n" +
                          "  <p>Key: <input type=\"text\" name=\"key\" /></p>\n" +
                          "  <input type=\"submit\" value=\"Submit\" />\n" +
                          "</form>\n" +
                          "<p>Get a value from key.</p>\n" +
                          "<form action=\"/kv/get\" method=\"get\">\n" +
                          "  <p>Key: <input type=\"text\" name=\"key\" /></p>\n" +
                          "  <input type=\"submit\" value=\"Submit\" />\n" +
                          "</form>\n" +
                          "<p>Update a (key, value) pair.</p>\n" +
                          "<form action=\"/kv/update\" method=\"post\">\n" +
                          "  <p>Key: <input type=\"text\" name=\"key\" /></p>\n" +
                          "  <p>Value: <input type=\"text\" name=\"value\" /></p>\n" +
                          "  <input type=\"submit\" value=\"Submit\" />\n" +
                          "</form>\n" +
                          "<p></p><p><a href=\"/kvman/countkey\">Count Key</a></p>\n" +
                          "<p></p><p><a href=\"/kvman/dump\">Print All Contents</a></p>\n" +
                          "</body>\n</html>\n")
}

// Method: POST
// Arguments: key=k&value=v
// Return: {"success":"<true or false>"}
func handle_kv_insert(wfile http.ResponseWriter, request *http.Request) {
    if request.Method != "POST" {
        bytes, _ := json.Marshal(map[string]string{"success":"false"})
        wfile.Write(bytes)
        return
    }

    err := request.ParseForm()
    if err != nil {
        bytes, _ := json.Marshal(map[string]string{"success":"false"})
        wfile.Write(bytes)
        return
    }

    keys, found_key := request.Form["key"]
    values, found_value := request.Form["value"]
    if !(found_key && found_value) {
        bytes, _ := json.Marshal(map[string]string{"success":"false"})
        wfile.Write(bytes)
        return
    }

    key := keys[0]
    value := values[0]
    if (len(key) == 0 || len(value) == 0) {
        bytes, _ := json.Marshal(map[string]string{"success":"false"})
        wfile.Write(bytes)
        return
    }

    mutex.Lock()
    defer mutex.Unlock()

    if model == PRIMARY_MODEL && crashed {
        ok := askBackupToRefresh()
        if !ok {
            bytes, _ := json.Marshal(map[string]string{"success":"false"})
            wfile.Write(bytes)
            return
        }
        crashed = false
    }

    if model == PRIMARY_MODEL {
        resp, err := http.PostForm("http://" + backup_ip + port + "/kv/insert", url.Values{"key":{key},"value":{value}})
        if err != nil {
            bytes, _ := json.Marshal(map[string]string{"success":"false"})
            wfile.Write(bytes)
            crashed = true
            return
        }
        defer resp.Body.Close()
    }

    ok := data.Insert(key, value)
    if !ok {
        bytes, _ := json.Marshal(map[string]string{"success":"false"})
        wfile.Write(bytes)
    } else {
        bytes, _ := json.Marshal(map[string]string{"success":"true"})
        wfile.Write(bytes)
    }
}

// Method: POST
// Arguments: key=k
// Return: {"success":"<true or false>","value":"<value deleted>"} 
func handle_kv_delete(wfile http.ResponseWriter, request *http.Request) {
    if request.Method != "POST" {
        bytes, _ := json.Marshal(map[string]string{"success":"false", "value":""})
        wfile.Write(bytes)
        return
    }

    err := request.ParseForm()
    if err != nil {
        bytes, _ := json.Marshal(map[string]string{"success":"false", "value":""})
        wfile.Write(bytes)
        return
    }

    keys, found_key := request.Form["key"]
    if !found_key {
        bytes, _ := json.Marshal(map[string]string{"success":"false", "value":""})
        wfile.Write(bytes)
        return
    }

    key := keys[0]
    if len(key) == 0 {
        bytes, _ := json.Marshal(map[string]string{"success":"false", "value":""})
        wfile.Write(bytes)
        return
    }

    mutex.Lock()
    defer mutex.Unlock()

    if model == PRIMARY_MODEL && crashed {
        ok := askBackupToRefresh()
        if !ok {
            bytes, _ := json.Marshal(map[string]string{"success":"false"})
            wfile.Write(bytes)
            return
        }
        crashed = false
    }

    if model == PRIMARY_MODEL {
        resp, err := http.PostForm("http://" + backup_ip + port + "/kv/delete", url.Values{"key":{key}})
        if err != nil {
            bytes, _ := json.Marshal(map[string]string{"success":"false"})
            wfile.Write(bytes)
            crashed = true
            return
        }
        defer resp.Body.Close()
    }

    ok, value := data.Delete(key)
    if !ok {
        bytes, _ := json.Marshal(map[string]string{"success":"false", "value":""})
        wfile.Write(bytes)
    } else {
        bytes, _ := json.Marshal(map[string]string{"success":"true", "value":value})
        wfile.Write(bytes)
    }
}

// Method: GET
// Arguments: key=k
// Return: {"success":"<true or false>","vaule":"<value>"}
func handle_kv_get(wfile http.ResponseWriter, request *http.Request) {
    if request.Method != "GET" {
        bytes, _ := json.Marshal(map[string]string{"success":"false", "value":""})
        wfile.Write(bytes)
        return
    }

    keys, found_key := request.URL.Query()["key"]
    if !found_key {
        bytes, _ := json.Marshal(map[string]string{"success":"false", "value":""})
        wfile.Write(bytes)
        return
    }

    key := keys[0]
    if len(key) == 0 {
        bytes, _ := json.Marshal(map[string]string{"success":"false", "value":""})
        wfile.Write(bytes)
        return
    }

    mutex.Lock()
    defer mutex.Unlock()

    ok, value := data.Get(key)
    if !ok {
        bytes, _ := json.Marshal(map[string]string{"success":"false", "value":""})
        wfile.Write(bytes)
    } else {
        bytes, _ := json.Marshal(map[string]string{"success":"true", "value":value})
        wfile.Write(bytes)
    }
}

// Method: POST
// Arguments: key=k&value=v
// Return: {"success":"<true or false>"}
func handle_kv_update(wfile http.ResponseWriter, request *http.Request) {
    if request.Method != "POST" {
        bytes, _ := json.Marshal(map[string]string{"success":"false"})
        wfile.Write(bytes)
        return
    }

    err := request.ParseForm()
    if err != nil {
        bytes, _ := json.Marshal(map[string]string{"success":"false"})
        wfile.Write(bytes)
        return
    }

    keys, found_key := request.Form["key"]
    values, found_value := request.Form["value"]
    if !(found_key && found_value) {
        bytes, _ := json.Marshal(map[string]string{"success":"false"})
        wfile.Write(bytes)
        return
    }

    key := keys[0]
    value := values[0]
    if len(key) == 0 || len(value) == 0 {
        bytes, _ := json.Marshal(map[string]string{"success":"false"})
        wfile.Write(bytes)
        return
    }

    mutex.Lock()
    defer mutex.Unlock()

    if model == PRIMARY_MODEL && crashed {
        ok := askBackupToRefresh()
        if !ok {
            bytes, _ := json.Marshal(map[string]string{"success":"false"})
            wfile.Write(bytes)
            return
        }
        crashed = false
    }

    if model == PRIMARY_MODEL {
        resp, err := http.PostForm("http://" + backup_ip + port + "/kv/update", url.Values{"key":{key},"value":{value}})
        if err != nil {
            bytes, _ := json.Marshal(map[string]string{"success":"false"})
            wfile.Write(bytes)
            crashed = true
            return
        }
        defer resp.Body.Close()
    }

    ok := data.Update(key, value)
    if !ok {
        bytes, _ := json.Marshal(map[string]string{"success":"false"})
        wfile.Write(bytes)
    } else {
        bytes, _ := json.Marshal(map[string]string{"success":"true"})
        wfile.Write(bytes)
    }
}

// Method: GET
// Arguments: None
// Return {"result":"<number of keys>"}
func handle_kvman_countkey(wfile http.ResponseWriter, request *http.Request) {
    mutex.Lock()
    defer mutex.Unlock()
    bytes, _ := json.Marshal(map[string]string{"result":strconv.Itoa(data.CountKey())})
    wfile.Write(bytes)
}

// Method: GET
// Arguments: None
// Return [["<key>","<value>"], ...]
func handle_kvman_dump(wfile http.ResponseWriter, request *http.Request) {
    mutex.Lock()
    defer mutex.Unlock()
    wfile.Write(data.Serialize())
}

// Method: GET
// Arguments: None
// Return None
func handle_kvman_shutdown(wfile http.ResponseWriter, request *http.Request) {
    os.Exit(0)
}

// Method: POST
// Arguments: value=serializedString
// Return None
func handle_kvman_refresh(wfile http.ResponseWriter, request *http.Request) {
    if model != BACKUP_MODEL || request.Method != "POST" {
        wfile.Write([]byte("false"))
        return
    }

    err := request.ParseForm()
    if err != nil {
        wfile.Write([]byte("false"))
        return
    }

    contents, found := request.Form["value"]
    if !found {
        wfile.Write([]byte("false"))
        return
    }

    content := contents[0]
    if len(content) == 0 {
        wfile.Write([]byte("false"))
        return
    }

    mutex.Lock()
    defer mutex.Unlock()
    data.Unserialize([]byte(content))
    wfile.Write([]byte("true"))
    return
}
