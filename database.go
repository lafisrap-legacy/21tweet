package main

// Database interface for 21tweet server
//
// The database interface consists of the data structures, constants and
// functions to handle all database request. It serves the database channel,
// that takes request from the websocket connections to the players

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
)

// Db_request ist the structure of requests send to the database component
// 	request		The name of the request
//	session		The player session
//	dataChan	The back channel for request results
//	parameter	Map with parameters
type Request struct {
	request   string
	dataChan  chan Data
	parameter Message
}

// StartDatabase initiates the database and create a request channel for the controller
func StartDatabase(config map[string]string, doneChan chan bool) chan Request {
	str := config["user"] + ":" + config["pass"] + "@tcp(127.0.0.1:3306)/" + config["database"]
	db, err := sql.Open("mysql", str)
	db.SetMaxOpenConns(50)
	err = db.Ping()
	if err != nil {
		panic(err)
	}

	requestChan := make(chan Request)

	go serveDatabase(db, requestChan, doneChan)

	fmt.Printf("21Tweets database server started. Waiting for requests ...\n")
	return requestChan
}

// serveDatabase is the internal event loop for database request
func serveDatabase(db *sql.DB, requestChan chan Request, doneChan chan bool) {

	for {
		select {
		case req := <-requestChan:
			go distributeRequest(db, req)
		}
	}
}

// distributeRequests matches the request with the corresponding function calls
func distributeRequest(db *sql.DB, req Request) {

	switch req.request {
	case "checkNames":
		req.dataChan <- checkNames(db, req.parameter)
	case "changeName":
		req.dataChan <- changeName(db, req.parameter)
	case "tweet":
		req.dataChan <- tweet(db, req.parameter)
	default:
		req.dataChan <- Data{}
	}
}

func checkNames(db *sql.DB, p Message) Data {
	ret := make(Data)

	cnt := 0
	for i := 0; ; i++ {
		name, ok := p["name"+strconv.Itoa(i)]
		if !ok {
			break
		}

		fmt.Println("Checking name ", name)

		err := db.QueryRow("select name from Names where name = ?", name).Scan(&name)
		switch {
		case err == sql.ErrNoRows:
			ret["name"+strconv.Itoa(cnt)] = name
			cnt++
		case err == nil:
		default:
			panic("checkNames: " + err.Error())
		}
	}

	return ret
}

func changeName(db *sql.DB, p Message) Data {
	var err error
	var res sql.Result
	var ok bool
	if ok {
		res, err = db.Exec("SELECT name FROM names")
		if err != nil {
			panic("checkNames: " + err.Error())
		} else {
			return Data{}
		}
	} else {
		err = errors.New("GameState parameter missing.")
	}

	fmt.Println(res)

	return Data{
		"error": err.Error(),
	}
}

func tweet(db *sql.DB, p Message) Data {
	var err error
	var res sql.Result
	var ok bool
	if ok {
		res, err = db.Exec("SELECT name FROM names")
		if err != nil {
			panic("checkNames: " + err.Error())
		} else {
			return Data{}
		}
	} else {
		err = errors.New("GameState parameter missing.")
	}

	fmt.Println(res)

	return Data{
		"error": err.Error(),
	}
}

/*
// Signup creates a player account, or if given a magic spell reactivates an
// existing account. This is meant for activation one player account on different
// devices and as a backup facility.
func Signup(db *sql.DB, p Cmd_data) []Cmd_data {
	var playerId, playerIdSha1 string

	magicSpell, ok := p["magicSpell"]

	if !ok {
		fmt.Println("Signing up new player ...")
		var id string
		for playerId == "" {
			// create player id (first get a random value, than get its SHA1 hash)
			playerId = GetHash(nil)
			playerIdSha1 = GetHash([]byte(playerId))

			// look if playerId is already in use (very unlikly)
			err := db.QueryRow("select id from players where id = ?", playerIdSha1).Scan(&id)
			switch {
			case err == sql.ErrNoRows:
			case err == nil:
				playerId = ""
			default:
				panic("signup: " + err.Error())
			}
		}

		fmt.Println("New pIDs: ", playerId, playerIdSha1)

		// insert new player id
		_, err := db.Exec("insert into players (id, beehive, magicspell, logins, gamestate) values (?,?,?,?,?)", playerIdSha1, "yaylaswiese", "", 0, "")
		if err != nil {
			panic("signup: " + err.Error())
		}
	} else {
		// search for magicSpell in players table, get player id
		fmt.Printf("Magic spell: %s\n", magicSpell)
	}

	return []Cmd_data{{
		"playerId": playerId,
	}}
}

// Signoff deletes a plaer account.
func Signoff(db *sql.DB, session *Session, p Cmd_data) []Cmd_data {

	var err error
	playerId := session.playerId
	_, err = db.Exec("DELETE FROM players WHERE id = ?", playerId)
	if err != nil {
		panic("signoff: " + err.Error())
	}

	// delete all other player related data here ...

	return []Cmd_data{}
}

// Login request retrieves the current game state, beehive and magic spell  of
// a player. A new session id is added by the controller.
func Login(db *sql.DB, p Cmd_data) []Cmd_data {

	playerId, ok := p["playerId"]
	playerIdSha1 := GetHash([]byte(playerId))
	var err error
	if ok && playerId != "" {

		// look if playerId is available
		var id, beehive, magicspell, gamestate string
		var logins int
		err = db.QueryRow("SELECT id, beehive, magicspell, logins, gamestate FROM players WHERE id = ?", playerIdSha1).Scan(&id, &beehive, &magicspell, &logins, &gamestate)
		switch {
		case err == sql.ErrNoRows:
			err = errors.New(ErrorMessages[1001])
			fmt.Println("Login error: ", err.Error(), playerIdSha1)
		case err == nil:
			// increment login counter
			_, err = db.Exec("UPDATE players SET logins = ? WHERE id = ?", logins+1, playerIdSha1)
			if err != nil {
				panic("login: UPDATE" + err.Error())
			}

			return []Cmd_data{{
				"beehive":    beehive,
				"magicSpell": magicspell,
				"gameState":  gamestate,
			}}
		default:
			panic("login SELECT: " + err.Error())
		}
	} else {
		err = errors.New("PlayerId parameter missing.")
	}

	return []Cmd_data{{
		"error": err.Error(),
	}}
}

// SaveState stores a JSON string into the players table, that contains the
// current game state
func SaveState(db *sql.DB, session *Session, p Cmd_data) []Cmd_data {

	var err error
	var res sql.Result
	gameState, ok := p["gameState"]
	if ok {
		fmt.Println("PlayerId:", session.playerId)
		res, err = db.Exec("UPDATE players SET gamestate = ? WHERE id = ?", gameState, session.playerId)
		if err != nil {
			panic("saveState: UPDATE" + err.Error())
		} else if r, _ := res.RowsAffected(); r == 0 {
			fmt.Println("SaveState: State not changed.")
			// err = errors.New("SaveState: PlayerId not found.")
			// 	r return 0 if PlayerId is not found or if UPDATE didn't change data, so err msg is incorrect
			return []Cmd_data{}
		} else {
			return []Cmd_data{}
		}
	} else {
		err = errors.New("GameState parameter missing.")
	}

	return []Cmd_data{{
		"error": err.Error(),
	}}
}

// GetBeehives returns a list with all currently available beehives
func GetBeehives(db *sql.DB) []Cmd_data {

	rows, err := db.Query("select name, shortname, id from beehives")
	if err != nil {
		panic("getBeehives: " + err.Error())
	}
	defer rows.Close()

	data := []Cmd_data{}
	for i := 0; rows.Next(); i++ {
		var name, shortname, id string
		err := rows.Scan(&name, &shortname, &id)
		if err != nil {
			panic(err)
		}
		data = append(data, Cmd_data{
			"name":      name,
			"shortname": shortname,
			"id":        id,
		})
	}
	return data
}

// LoginBeehive moves a player account to a new beehive
func LoginBeehive(db *sql.DB, p Cmd_data) []Cmd_data {

	beehive, ok1 := p["beehive"]
	secret1, ok2 := p["secret"]
	var err error

	if ok1 && ok2 {

		var id, secret2, shortname string
		err = db.QueryRow("select id, secret, shortname from beehives where shortname = ?", beehive).Scan(&id, &secret2, &shortname)
		switch {
		case err == sql.ErrNoRows:
			err = errors.New("Beehive '" + beehive + "' not found.")
		case err != nil:
			panic("loginBeehive: " + err.Error())
		default:
			if secret1 == secret2 {
				return []Cmd_data{{
					"id":        id,
					"shortname": shortname,
				}}

			} else {
				err = errors.New("Wrong secret.")
			}
		}
	} else {
		err = errors.New("Parameter missing: beehive or secret.")
	}

	return []Cmd_data{{
		"error": err.Error(),
	}}
}

*/
