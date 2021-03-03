package mywabak

import (
    "net/http"
    "encoding/json"
    // "time"
    // "strconv"
    // "strings"
    "errors"
    "fmt"
    "log"
    "context"
    "github.com/jackc/pgx"
    "github.com/jackc/pgx/pgxpool"
    "mywabak/webservice/db"
    // "mywabak/webservice/auth"
    "mywabak/webservice/util"
)

type Wbkcase struct {
	Name string 	    `json:"name"`
    Peopleident string  `json:"peopleident"`
    Contactto string    `json:"contactto"`
    Lastcontact string  `json:"lastcontact"`
    Symptoms []string   `json:"symptoms"`
    Onset string        `json:"onset"`
    Workloc string      `json:"workloc"`
    Remarks string      `json:"remarks"`
    Casetype string     `json:"casetype"`
    Caseorigin string   `json:"caseorigin"`
    Livedeadstat string `json:"livedeadstat"`
    Causeofdeath string `json:"causeofdeath"`
}

type Identity struct {
    Ident string    `json:"ident"`
}

type People struct {
	Name string 	      `json:"name"`
    Ident string          `json:"ident"`
    Gender string         `json:"gender"`
    Dob string            `json:"dob"` //kiv change to time.Time type
    Nationality string    `json:"nationality"`
    Race string           `json:"race"`
    Tel string            `json:"tel"`
    Address string        `json:"address"`  
    State string          `json:"state"` 
    District string       `json:"district"`
    Locality string       `json:"locality"`
    Occupation string     `json:"occupation"`
    Isgovemp bool         `json:"isgovemp"`
}

type PosCases struct {
	Peoples []People 	`json:"peoples"`
}

type CloseContacts struct {
	Peoples []People 	`json:"peoples"`
}

type HSO struct {
    Id int              `json:"id"`
    Begindt string      `json:"begindt"`
    Enddt string        `json:"enddt"`
    Extension int       `json:"extension"`
    Address string      `json:"address"`
    State string        `json:"state"`
    District string     `json:"district"`
    Locality string     `json:"locality"`
    Peopleident string  `json:"peopleident"`
    Idents []string     `json:"idents"`
}

func GetPosCasesPCR(conn *pgxpool.Pool, casename string) ([]byte, error) {
	sql :=
		`select p.name
		 from wbk.wbkcase c
		     join wbk.wbkcase_people cp
			   on c.id = cp.wbkcaseid
			 join wbk.people p
			   on cp.peopleident = p.ident
			 join wbk.sampling s
			   on p.ident = s.peopleident
		 where c.name=$1
		   and s.wbkcaseid = c.id
		   and s.samplingres::text = 'positive'
           and s.samplingtype::text = 'rtpcr'`

	rows, err := conn.Query(context.Background(), sql, casename)
	if err != nil {
		return nil, err
	}

	var posCases PosCases
	for rows.Next() {
		var name string

		err = rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		posCase := People{
			Name: name,
		}
		posCases.Peoples = append(posCases.Peoples, posCase)
	}

	outputJson, err := json.MarshalIndent(posCases, "", "\t")
    return outputJson, err	
}

func GetPosCasesPCRHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[GetPosCasesPCRHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var wbkcase Wbkcase
    err := json.NewDecoder(r.Body).Decode(&wbkcase)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    posCasesJson, err := GetPosCasesPCR(db.Conn, wbkcase.Name)
    if err != nil {
        if err == pgx.ErrNoRows {
            log.Print("Positive cases not found in database")
        }
        util.SendInternalServerErrorStatus(w, err)
        return 
    }
    fmt.Printf("%s\n", posCasesJson)
    fmt.Fprintf(w, "%s", posCasesJson)
}

func GetCloseContacts(conn *pgxpool.Pool, casename string) ([]byte, error) {
	sql :=
		`select p.name
		 from wbk.wbkcase c
		     join wbk.wbkcase_people cp
			   on c.id = cp.wbkcaseid
			 join wbk.people p
			   on cp.peopleident = p.ident
			 left join wbk.sampling s
			   on p.ident = s.peopleident
		 where c.name=$1
		   and s.wbkcaseid = c.id
		   and 
             (s.samplingres::text != 'positive'
             or 
             (s.samplingres::text = 'positive'
               and 
             s.samplingtype::text != 'rtpcr'))`

	rows, err := conn.Query(context.Background(), sql, casename)
	if err != nil {
		return nil, err
	}

	var closeContacts CloseContacts
	for rows.Next() {
		var name string

		err = rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		closeContact := People{
			Name: name,
		}
		closeContacts.Peoples = append(closeContacts.Peoples, closeContact)
	}

	outputJson, err := json.MarshalIndent(closeContacts, "", "\t")
    return outputJson, err	
}

func GetCloseContactsHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[GetCloseContactsHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var wbkcase Wbkcase
    err := json.NewDecoder(r.Body).Decode(&wbkcase)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    closeContactsJson, err := GetCloseContacts(db.Conn, wbkcase.Name)
    if err != nil {
        if err == pgx.ErrNoRows {
            log.Print("Positive cases not found in database")
        }
        util.SendInternalServerErrorStatus(w, err)
        return 
    }
    fmt.Printf("%s\n", closeContactsJson)
    fmt.Fprintf(w, "%s", closeContactsJson)
}

func GetPeopleBasic(conn *pgxpool.Pool, ident string) ([]byte, error) {
    sqlSelect := 
		`select name from wbk.people
		 where ident=$1`

	row := conn.QueryRow(context.Background(), sqlSelect,
				ident)
	var name string				
	err := row.Scan(&name)				
	if err != nil {
		// People Ident doesn't exist, 
		// so can sign up a new account.
	    if err == pgx.ErrNoRows { 
			
			peopleNotFound := People{
                Ident: "NOTFOUND",
            }
            outputJson, err := json.MarshalIndent(peopleNotFound, "", "\t")
			return outputJson, err
		} 
		return nil, err
	}   

    people := People{
        Name: name,
        Ident: ident,
    }
    outputJson, err := json.MarshalIndent(people, "", "\t")
			return outputJson, err
}

func GetPeopleBasicHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[GetPeopleBasicHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var identity Identity
    err := json.NewDecoder(r.Body).Decode(&identity)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    peopleJson, err := GetPeopleBasic(db.Conn, identity.Ident)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }
    fmt.Printf("%s\n", peopleJson)
    fmt.Fprintf(w, "%s", peopleJson)
}

func AddPeopleToWbkcase(conn *pgxpool.Pool, c Wbkcase) error {
    if c.Name == "" || c.Peopleident == "" {
        return errors.New(util.INPUT_PARAMS_NOT_INITIALIZED)
    }
    
    sql := 
        `insert into wbk.wbkcase_people
        (
            wbkcaseid, peopleident
        )
        select c.id, $1
        from wbk.wbkcase c
        where c.name=$2`    

    _, err := conn.Exec(context.Background(), sql, 
        c.Peopleident, c.Name)
    if err != nil {
        return err
    }
    return nil
}

func AddPeopleToWbkcaseHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[AddPeopleToWbkcaseHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var wbkcase Wbkcase
    err := json.NewDecoder(r.Body).Decode(&wbkcase)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = AddPeopleToWbkcase(db.Conn, wbkcase)
    if err != nil {                
        if err.Error() == util.INPUT_PARAMS_NOT_INITIALIZED {
            util.SendBadReqStatus(w, err)
        } else {
            util.SendInternalServerErrorStatus(w, err)
        }
        return 
    }    
}

func UpdatePeopleInWbkcase(conn *pgxpool.Pool, c Wbkcase) error {
    sql := 
        `update wbk.wbkcase_people
           set contactto=$1, lastcontact=$2, symptoms=$3, onset=$4, 
             workloc=$5, remarks=$6, casetype=$7, livedeadstat=$8,
             causeofdeath=$9
           where wbkcaseid=(select c.id
                            from wbk.wbkcase c
                            where c.name=$10)
             and peopleident=$11`

    _, err := conn.Exec(context.Background(), sql,
        c.Contactto, c.Lastcontact, c.Symptoms, c.Onset,
        c.Workloc, c.Remarks, c.Casetype, c.Livedeadstat, 
        c.Causeofdeath, c.Name, c.Peopleident)                         
    if err != nil {
        return err
    }
    return nil
}

func UpdatePeopleInWbkcaseHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[UpdatePeopleInWbkcaseHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var wbkcase Wbkcase
    err := json.NewDecoder(r.Body).Decode(&wbkcase)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = UpdatePeopleInWbkcase(db.Conn, wbkcase)
    if err != nil {                
        if err.Error() == util.INPUT_PARAMS_NOT_INITIALIZED {
            util.SendBadReqStatus(w, err)
        } else {
            util.SendInternalServerErrorStatus(w, err)
        }
        return 
    }    
}

func DelPeopleFromWbkcase(conn *pgxpool.Pool, wbkc Wbkcase) error {
    if wbkc.Name == "" || wbkc.Peopleident == "" {
        return errors.New(util.INPUT_PARAMS_NOT_INITIALIZED)
    }
    
    sql := 
        `delete from wbk.wbkcase_people
         where wbkcaseid=(
             select c.id
             from wbk.wbkcase c
             where c.name=$1
         )
         and peopleident=$2`    

    _, err := conn.Exec(context.Background(), sql, 
        wbkc.Name, wbkc.Peopleident)
    if err != nil {
        return err
    }
    return nil
}

func DelPeopleFromWbkcaseHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[DelPeopleFromWbkcaseHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var wbkcase Wbkcase
    err := json.NewDecoder(r.Body).Decode(&wbkcase)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = DelPeopleFromWbkcase(db.Conn, wbkcase)
    if err != nil {                
        if err.Error() == util.INPUT_PARAMS_NOT_INITIALIZED {
            util.SendBadReqStatus(w, err)
        } else {
            util.SendInternalServerErrorStatus(w, err)
        }
        return 
    }    
}

func AddNewPeople(conn *pgxpool.Pool, p People) error {
    sql := 
        `insert into wbk.people
        (
            ident, name, gender, dob, nationality, race, tel,
            address, state, district, locality, occupation,
            isgovemp
        )
        values
        (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
            $11, $12, $13
        )`
        
    _, err := conn.Exec(context.Background(), sql, 
        p.Ident, p.Name, p.Gender, p.Dob, p.Nationality, 
        p.Race, p.Tel, p.Address, p.State, p.District, 
        p.Locality, p.Occupation, p.Isgovemp)
    if err != nil {
        return err
    }
    return nil
}

func AddNewPeopleHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[AddNewPeopleHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }

    var p People
    err := json.NewDecoder(r.Body).Decode(&p)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = AddNewPeople(db.Conn, p)
    if err != nil {                        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }    
}

func UpdatePeople(conn *pgxpool.Pool, p People) error {
    sql := 
        `update wbk.people
           set name=$1, gender=$2, dob=$3, nationality=$4, 
             race=$5, tel=$6, address=$7, state=$8, district=$9,
             locality=$10, occupation=$11, isgovemp=$12
           where ident=$13`

    _, err := conn.Exec(context.Background(), sql,
        p.Name, p.Gender, p.Dob, p.Nationality, p.Race,
        p.Tel, p.Address, p.State, p.District, p.Locality,
        p.Occupation, p.Isgovemp, p.Ident)                         
    if err != nil {
        return err
    }
    return nil
}

func UpdatePeopleHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[UpdatePeopleHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }

    var p People
    err := json.NewDecoder(r.Body).Decode(&p)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = UpdatePeople(db.Conn, p)
    if err != nil {                        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }    
}

func AddNewHSO(conn *pgxpool.Pool, hso HSO) error {    
    sql := 
        `insert into wbk.hso
        (
            peopleident, begindt, enddt, extension, 
            address, state, district, locality
        )
        values
        (
            $1, $2, $3, $4, $5, $6, $7, $8
        )`        
            
    for i,_ := range hso.Idents {
        _, err := conn.Exec(context.Background(), sql, 
            hso.Idents[i], hso.Begindt, hso.Enddt, hso.Extension, 
            hso.Address, hso.State, hso.District, hso.Locality)
        if err != nil {
            return err
        }
    }    
    return nil
}

func AddNewHSOHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[AddNewHSOHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }

    var hso HSO
    err := json.NewDecoder(r.Body).Decode(&hso)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = AddNewHSO(db.Conn, hso)
    if err != nil {                        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }    
}

func DelHSO(conn *pgxpool.Pool, id int) error {
    sql := 
        `delete from wbk.hso
         where id=$1`

    _, err := conn.Exec(context.Background(), sql, id)
    if err != nil {
        return err
    }
    return nil
}

func DelHSOHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[DelHSOHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }

    var hso HSO
    err := json.NewDecoder(r.Body).Decode(&hso)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = DelHSO(db.Conn, hso.Id)
    if err != nil {                        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }    
}

func UpdateHSO(conn *pgxpool.Pool, hso HSO) error {
    sql :=
        `update wbk.hso
           set peopleident=$1, begindt=$2, enddt=$3, extension=$4,
             address=$5, state=$6, district=$7, locality=$8
           where id=$9`
    
    _, err := conn.Exec(context.Background(), sql,                 
        hso.Peopleident, hso.Begindt, hso.Enddt, hso.Extension,
        hso.Address, hso.State, hso.District, hso.Locality, hso.Id)
    if err != nil {
        return err
    }
    return nil
}

func UpdateHSOHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[UpdateHSOHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }

    var hso HSO
    err := json.NewDecoder(r.Body).Decode(&hso)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = UpdateHSO(db.Conn, hso)
    if err != nil {                        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }    
}