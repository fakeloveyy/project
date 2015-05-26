package main

import (
    "encoding/json"
    "errors"
    "fmt"
    "net/http"
    "os"
)

const (
    ERROR_MODEL = iota
    PRIMARY_MODEL
    BACKUP_MODEL
)
var model int
var primary_ip, backup_ip, port string

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
        resp, err := http.Get("http://" + primary_ip + port + "/kvman/shutdown")
        if err != nil {
            return
        }
        defer resp.Body.Close()
    } else {
        resp, err := http.Get("http://" + backup_ip + port + "/kvman/shutdown")
        if err != nil {
            return
        }
        defer resp.Body.Close()
    }
}
