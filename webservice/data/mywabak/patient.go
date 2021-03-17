package mywabak

import (
    "os"
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
    "github.com/jackc/pgconn"
    "mywabak/webservice/db"
    // "mywabak/webservice/auth"
    "mywabak/webservice/util"
)

type Wbkcase struct {
	Casename string	        `json:"casename"`
    Peopleident string      `json:"peopleident"`
    Contactto string        `json:"contactto"`
    Lastcontact string      `json:"lastcontact"`
    Symptoms []string       `json:"symptoms"`
    Onset string            `json:"onset"`
    Workloc string          `json:"workloc"`
    Remarks string          `json:"remarks"`
    Casetype string         `json:"casetype"`
    Caseorigin string       `json:"caseorigin"`
    Livedeadstat string     `json:"livedeadstat"`
    Causeofdeath string     `json:"causeofdeath"`
    AssignedToIk string     `json:"assignedToIk"`
    HasBeenVerified bool    `json:"hasBeenVerified"`
    VerifiedBy string       `json:"verifiedBy"`
}

type WbkcasePeopleError struct {
    Err string          `json:"err"`
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
    Comorbid []string     `json:"comorbid"`
}

type CloseContactIn struct {
	Name string 	      `json:"name"`
    Ident string          `json:"ident"`
    //Unverified Ident
    UVIdent string        `json:"uvIdent"` 
    Gender string         `json:"gender"`
    Dob string            `json:"dob"` //kiv change to time.Time type
    Nationality string    `json:"nationality"`
    Race string           `json:"race"`
    Tel string            `json:"tel"`    
    Occupation string     `json:"occupation"`
    Comorbid []string     `json:"comorbid"`    
    Contactto string      `json:"contactto"`
    Lastcontact string    `json:"lastcontact"`
    Symptoms []string     `json:"symptoms"`
    Onset string          `json:"onset"`
    Workloc string        `json:"workloc"`
}

type CloseContactsIn struct {
    Address string                 `json:"address"`
    Locality string                `json:"locality"`
    District string                `json:"district"`
    State string                   `json:"state"`
    CloseContacts []CloseContactIn `json:"closeContacts"`
}

// KIV to remove this struct
type CloseContactOut struct {
	Name string 	      `json:"name"`
    Ident string          `json:"ident"`
    Gender string         `json:"gender"`
    Dob string            `json:"dob"` //kiv change to time.Time type
    Nationality string    `json:"nationality"`
    Race string           `json:"race"`
    Tel string            `json:"tel"`    
    Occupation string     `json:"occupation"`
    Comorbid []string     `json:"comorbid"`
    Address string        `json:"address"`
    Locality string       `json:"locality"`
    District string       `json:"district"`
    State string          `json:"state"`
}

type NewCloseContactOut struct {
    Hasbeenreviewed bool  `json:"hasbeenreviewed"`
	Name string 	      `json:"name"`
    Ident string          `json:"ident"`
    Gender string         `json:"gender"`
    Dob string            `json:"dob"` //kiv change to time.Time type
    Nationality string    `json:"nationality"`
    Race string           `json:"race"`
    Tel string            `json:"tel"`    
    Occupation string     `json:"occupation"`
    Comorbid []string     `json:"comorbid"`
    Address string        `json:"address"`
    Locality string       `json:"locality"`
    District string       `json:"district"`
    State string          `json:"state"`
}

type CloseContactsOut struct {
    CloseContacts []CloseContactOut   `json:"closeContacts"`
}

type NewCloseContactsOut struct {
    CloseContacts []NewCloseContactOut   `json:"closeContacts"`
}

type WbkcaseMetadata struct {
    Mode string             `json:"mode"`
    Casename string	        `json:"casename"`
    AssignedToIk string     `json:"assignedToIk"`
    HasBeenVerified bool    `json:"hasBeenVerified"`
    VerifiedBy string       `json:"verifiedBy"`
}

type CloseContactRegistration struct {
    Wbkcase WbkcaseMetadata                      `json:"wbkcase"`
    CloseContactRegs []CloseContactsIn    `json:"closeContactRegs"`
}

type CloseContactRegStatus struct {
    CCRegStatus string      `json:"ccRegStatus"`
}

type CloseContactSearchStatus struct {
    CCSearchStatus string      `json:"ccSearchStatus"`
}

type PosCases struct {
	Peoples []People 	`json:"peoples"`
}

type CloseContacts struct {
	Peoples []People 	`json:"cc"`
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
		   and s.samplingres::text = 'Positive'
           and s.samplingtype::text = 'RT-PCR'`

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
    posCasesJson, err := GetPosCasesPCR(db.Conn, wbkcase.Casename)
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

// This function return all close contacts in a case,
// whose PCR is negative.
// Close contacts whose PCR is positive are excluded.
func GetCloseContacts(conn *pgxpool.Pool, casename string) ([]byte, error) {
	sql :=
		`select p.name, p.ident
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
             (s.samplingres::text != 'Positive'
             or 
             (s.samplingres::text = 'Positive'
               and 
             s.samplingtype::text != 'RT-PCR'))`

	rows, err := conn.Query(context.Background(), sql, casename)
	if err != nil {
        if err == pgx.ErrNoRows {
            log.Print("CC not found in database")
            var noCC CloseContacts
            outputJson, err := json.MarshalIndent(noCC, "", "")            
            return outputJson, err
        }
		return nil, err
	}

	var closeContacts CloseContacts
	for rows.Next() {
		var name string
        var ident string

		err = rows.Scan(&name, &ident)
		if err != nil {
			return nil, err
		}
		closeContact := People{
			Name: name,
            Ident: ident,
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
    closeContactsJson, err := GetCloseContacts(db.Conn, wbkcase.Casename)
    if err != nil {        
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
    if c.Casename == "" || c.Peopleident == "" {
        return errors.New(util.INPUT_PARAMS_NOT_INITIALIZED)
    }
    var err error

    if c.VerifiedBy == "" {
        sql := 
            `insert into wbk.wbkcase_people
            (
                wbkcaseid, peopleident, assignedtoik, hasbeenverified            
            )
            select c.id, $1, $3, $4, $5
            from wbk.wbkcase c
            where c.name=$2`    

        _, err = conn.Exec(context.Background(), sql, 
            c.Peopleident, c.Casename, c.AssignedToIk, false)
    } else {
        sql := 
            `insert into wbk.wbkcase_people
            (
                wbkcaseid, peopleident, assignedtoik, 
                hasbeenverified, verifiedby
            )
            select c.id, $1, $3, $4, $5
            from wbk.wbkcase c
            where c.name=$2`    
    
        _, err = conn.Exec(context.Background(), sql, 
            c.Peopleident, c.Casename, c.AssignedToIk, 
            c.HasBeenVerified, c.VerifiedBy)
    }
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
            return
        }         
        
        if pgerr, ok := err.(*pgconn.PgError); ok {
			if pgerr.ConstraintName == "wbkcase_people_wbkcaseid_peopleident_key" {
				fmt.Fprintf(os.Stderr, "Unable to insert a new entry into wbkcase_people, because an ident for this case already exists: %v\n", pgerr)
                wpErr := WbkcasePeopleError{
                    Err: "IDEXISTS",
                }
                outputJson, err := json.MarshalIndent(wpErr, "", "")
                if err != nil {
                    util.SendInternalServerErrorStatus(w, err)
                }
                fmt.Fprintf(w, "%s", outputJson)
			} else {
				fmt.Fprintf(os.Stderr, "Unexpected postgres error trying to insert a wbkcase_people entry: %v\n", pgerr)
                util.SendInternalServerErrorStatus(w, pgerr)
			}
		} else {
			fmt.Fprintf(os.Stderr, "Unexpected error trying to insert a wbkcase_people entry: %v\n", err)
            util.SendInternalServerErrorStatus(w, err)
		}
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
        c.Causeofdeath, c.Casename, c.Peopleident)                         
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
    if wbkc.Casename == "" || wbkc.Peopleident == "" {
        return errors.New(util.INPUT_PARAMS_NOT_INITIALIZED)
    }
    
    sql := 
        `delete from wbk.wbkcase_people
         where wbkcaseid=(
             select c.id
             from wbk.wbkcase c
             where c.name=$1
         )
         and peopleident=$2
         and (select 1
              from wbk.wbkcase_people cp
              where cp.contactto=$2) is null`    

    _, err := conn.Exec(context.Background(), sql, 
        wbkc.Casename, wbkc.Peopleident)
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

func UpsertPeople(conn *pgxpool.Pool, p People) error {
    // sql :=
	// 		`insert into acd.house
	// 		(
	// 			tarikhacd, locality, district, state,
	// 			bilrumahk
	// 		)
	// 		values
	// 		(
	// 			$1, $2, $3, $4, $5
	// 		) 
	// 		on conflict on constraint house_tarikhacd_locality_key
	// 		do 
	// 			update set bilrumahk=house.bilrumahk+1
	// 			where house.tarikhacd=$1
	// 			and house.locality=$2
	// 			and house.district=$3
	// 			and house.state=$4`
    
    // sql := 
    //     `insert into wbk.people
    //     (
    //         ident, name, gender, dob, nationality, race, tel,
    //         address, state, district, locality, occupation
            
    //     )
    //     values
    //     (
    //         $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
    //         $11, $12
    //     )`

    sql := 
        `insert into wbk.people
        (
            ident, name, gender, dob, nationality, race, tel,
            address, state, district, locality, occupation
            
        )
        values
        (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
            $11, $12
        )
        on conflict on constraint people_ident_key
			do 
				update set name=$2, gender=$3, dob=$4, nationality=$5,
                  race=$6, tel=$7, address=$8, state=$9, district=$10,
                  locality=$11, occupation=$12
				where people.ident=$1`
        
    _, err := conn.Exec(context.Background(), sql, 
        p.Ident, p.Name, p.Gender, p.Dob, p.Nationality, 
        p.Race, p.Tel, p.Address, p.State, p.District, 
        p.Locality, p.Occupation)
    if err != nil {
        return err
    }
    return nil
}

func UpsertPeopleHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[UpsertPeopleHandler] request received")    

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
    err = UpsertPeople(db.Conn, p)
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
             locality=$10, occupation=$11
           where ident=$12`

    _, err := conn.Exec(context.Background(), sql,
        p.Name, p.Gender, p.Dob, p.Nationality, p.Race,
        p.Tel, p.Address, p.State, p.District, p.Locality,
        p.Occupation, p.Ident)                         
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

func DISABLED_RegNewCloseContactMode(conn *pgxpool.Pool, c WbkcaseMetadata, cc CloseContactIn,
    address string, locality string, district string, state string) error {

    // INSERT PEOPLE
    sql := 
        `insert into wbk.people
        (
            ident, name, gender, dob, nationality, race, tel,
            address, state, district, locality, occupation
            
        )
        values
        (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
            $11, $12
        )        
        on conflict on constraint people_ident_key
			do 
				update set name=$2, gender=$3, dob=$4, nationality=$5,
                  race=$6, tel=$7, address=$8, state=$9, district=$10,
                  locality=$11, occupation=$12
				where people.ident=$1`
        
    _, err := conn.Exec(context.Background(), sql, 
        cc.Ident, cc.Name, cc.Gender, cc.Dob, cc.Nationality, 
        cc.Race, cc.Tel, address, state, district, 
        locality, cc.Occupation)
    if err != nil {
        return err
    }    

    // INSERT WBKCASE_PEOPLE
    if c.Casename == "" || cc.Ident == "" {
        return errors.New(util.INPUT_PARAMS_NOT_INITIALIZED)
    }

    if c.VerifiedBy == "" {
        sql := 
            `insert into wbk.wbkcase_people
            (
                wbkcaseid, peopleident, assignedtoik, hasbeenverified            
            )
            select c.id, $1, $3, $4
            from wbk.wbkcase c
            where c.name=$2
            on conflict on constraint wbkcase_people_wbkcaseid_peopleident_key
              do
                update set assignedtoik=$3, hasbeenverified=$4
                where wbkcase_people.wbkcaseid=(select c.id
                                        from wbk.wbkcase c
                                        where c.name=$2)
                  and wbkcase_people.peopleident=$1`    

        _, err = conn.Exec(context.Background(), sql, 
            cc.Ident, c.Casename, c.AssignedToIk, false)
    } else {
        sql := 
            `insert into wbk.wbkcase_people
            (
                wbkcaseid, peopleident, assignedtoik, 
                hasbeenverified, verifiedby
            )
            select c.id, $1, $3, $4, $5
            from wbk.wbkcase c
            where c.name=$2
            on conflict on constraint wbkcase_people_wbkcaseid_peopleident_key
              do
                update set assignedtoik=$3, hasbeenverified=$4, 
                  verifiedby=$5
                where wbkcase_people.wbkcaseid=(select c.id
                                        from wbk.wbkcase c
                                        where c.name=$2)
                  and wbkcase_people.peopleident=$1`    
    
        _, err = conn.Exec(context.Background(), sql, 
            cc.Ident, c.Casename, c.AssignedToIk, 
            c.HasBeenVerified, c.VerifiedBy)
    }
    if err != nil {
        return err
    }

    return nil
}

// Mode 1: When registration form is filled by close contact.
// Mode 2: When registration form is filled by close contact 
//         and then by healthcare staff.
// Mode 3: When registration form is filled directly by healthcare staff.
func RegNewCloseContact(conn *pgxpool.Pool, c WbkcaseMetadata, cc CloseContactIn,
    address string, locality string, district string, state string) error {

    if c.Casename == "" || cc.Ident == "" {
        return errors.New(util.INPUT_PARAMS_NOT_INITIALIZED)
    }
    
    var err error     

    /* 
       ========================
       UPSERT PEOPLETEMP/PEOPLE 
       ========================
    */ 
    if c.Mode == "1" {    
        // UPSERT PEOPLETEMP
        sql := 
            `insert into wbk.peopletemp
            (
                wbkcaseid, ident, name, gender, dob, nationality, race, tel,
                address, state, district, locality, occupation,
                comorbid                
            )
            select c.id, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
              $11, $12, $13
            from wbk.wbkcase c
            where c.name=$14        
            on conflict on constraint peopletemp_wbkcaseid_ident_key
                do 
                    update set name=$2, gender=$3, dob=$4, nationality=$5,
                    race=$6, tel=$7, address=$8, state=$9, district=$10,
                    locality=$11, occupation=$12, comorbid=$13
                    where peopletemp.ident=$1
                      and peopletemp.wbkcaseid=(select c.id
                                    from wbk.wbkcase c
                                    where c.name=$14)`
            
        _, err = conn.Exec(context.Background(), sql, 
            cc.Ident, cc.Name, cc.Gender, cc.Dob, cc.Nationality, 
            cc.Race, cc.Tel, address, state, district, 
            locality, cc.Occupation, cc.Comorbid, c.Casename)
        if err != nil {
            return err
        }

    } else if c.Mode == "2" || c.Mode == "3" {
        // UPSERT PEOPLE
        sql := 
            `insert into wbk.people
            (
                ident, name, gender, dob, nationality, race, tel,
                address, state, district, locality, occupation,
                comorbid
            )
            values
            (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
                $11, $12, $13
            )        
            on conflict on constraint people_ident_key
                do 
                    update set name=$2, gender=$3, dob=$4, nationality=$5,
                    race=$6, tel=$7, address=$8, state=$9, district=$10,
                    locality=$11, occupation=$12, comorbid=$13
                    where people.ident=$1`
            
        _, err = conn.Exec(context.Background(), sql, 
            cc.Ident, cc.Name, cc.Gender, cc.Dob, cc.Nationality, 
            cc.Race, cc.Tel, address, state, district, 
            locality, cc.Occupation, cc.Comorbid)
        if err != nil {
            return err
        }   
    } 
    
    /* 
       =======================
       DELETE PEOPLETEMP ENTRY 
       =======================
    */ 
    if c.Mode == "2" {
        sql := `delete from wbk.peopletemp
                where ident=$1`
       
        _, err = conn.Exec(context.Background(), sql, 
            cc.UVIdent)
        if err != nil {
            return err
        }
    }
    if c.Mode == "3" {
        sql := `delete from wbk.peopletemp
                where ident=$1`

        _, err = conn.Exec(context.Background(), sql, 
            cc.Ident)
        if err != nil {
            return err
        }
    }

    /* 
       =====================
       UPSERT WBKCASE_PEOPLE
       =====================
    */     
    if c.VerifiedBy == "" {
        // Mode 1
        sql := 
            `insert into wbk.wbkcase_people
            (
                wbkcaseid, peopleident, assignedtoik, hasbeenverified            
            )
            select c.id, $1, $3, $4
            from wbk.wbkcase c
            where c.name=$2
            on conflict on constraint wbkcase_people_wbkcaseid_peopleident_key
              do
                update set assignedtoik=$3, hasbeenverified=$4
                where wbkcase_people.wbkcaseid=(select c.id
                                        from wbk.wbkcase c
                                        where c.name=$2)
                  and wbkcase_people.peopleident=$1`    

        _, err = conn.Exec(context.Background(), sql, 
            cc.Ident, c.Casename, c.AssignedToIk, false)
    } else {
        // Mode 2 & Mode 3
        // ---------------
        // Cover both implicit & explicit no symptoms
        if (len(cc.Symptoms)==0 || cc.Symptoms[0]=="nosx") {
            sql := 
                `insert into wbk.wbkcase_people
                (
                    wbkcaseid, peopleident, assignedtoik, 
                    hasbeenverified, verifiedby,
                    contactto, lastcontact, symptoms, onset, workloc
                )
                select c.id, $1, $3, $4, $5, $6, $7, $8, null, $9
                from wbk.wbkcase c
                where c.name=$2
                on conflict on constraint wbkcase_people_wbkcaseid_peopleident_key
                do
                    update set assignedtoik=$3, hasbeenverified=$4, 
                    verifiedby=$5, 
                    contactto=$6, lastcontact=$7, symptoms=$8, 
                    onset=null, workloc=$9
                    where wbkcase_people.wbkcaseid=(select c.id
                                            from wbk.wbkcase c
                                            where c.name=$2)
                    and wbkcase_people.peopleident=$1`    
        
            _, err = conn.Exec(context.Background(), sql, 
                cc.Ident, c.Casename, c.AssignedToIk, 
                c.HasBeenVerified, c.VerifiedBy,
                cc.Contactto, cc.Lastcontact, cc.Symptoms,
                cc.Workloc)
        } else {
            sql := 
                `insert into wbk.wbkcase_people
                (
                    wbkcaseid, peopleident, assignedtoik, 
                    hasbeenverified, verifiedby,
                    contactto, lastcontact, symptoms, onset, workloc
                )
                select c.id, $1, $3, $4, $5, $6, $7, $8, $9, $10
                from wbk.wbkcase c
                where c.name=$2
                on conflict on constraint wbkcase_people_wbkcaseid_peopleident_key
                do
                    update set assignedtoik=$3, hasbeenverified=$4, 
                    verifiedby=$5, 
                    contactto=$6, lastcontact=$7, symptoms=$8, 
                    onset=$9, workloc=$10
                    where wbkcase_people.wbkcaseid=(select c.id
                                            from wbk.wbkcase c
                                            where c.name=$2)
                      and wbkcase_people.peopleident=$1`    
        
            _, err = conn.Exec(context.Background(), sql, 
                cc.Ident, c.Casename, c.AssignedToIk, 
                c.HasBeenVerified, c.VerifiedBy,
                cc.Contactto, cc.Lastcontact, cc.Symptoms,
                cc.Onset, cc.Workloc)
        }
    }
    if err != nil {
        return err
    }

    /* 
       =================================================
       DELETE WBKCASE_PEOPLE ENTRY WITH UNVERIFIED IDENT
       =================================================
    */ 
    if c.Mode == "2" && cc.Ident != cc.UVIdent {
        sql := `delete from wbk.wbkcase_people
                where wbkcaseid=(select c.id
                                from wbk.wbkcase c
                                where c.name=$2)
                  and peopleident=$1`
       
        _, err = conn.Exec(context.Background(), sql, 
            cc.UVIdent, c.Casename)
        if err != nil {
            return err
        }
    }



    return nil
}

func RegNewCloseContactHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[RegNewCloseContactHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var ccr CloseContactRegistration
    err := json.NewDecoder(r.Body).Decode(&ccr)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }

    db.CheckDbConn()
    for _, reg := range ccr.CloseContactRegs {
        for _, cc := range reg.CloseContacts {
            
            err = RegNewCloseContact(db.Conn, ccr.Wbkcase, cc, 
                reg.Address, reg.Locality, reg.District, reg.State)            
            if err != nil {                
                if err.Error() == util.INPUT_PARAMS_NOT_INITIALIZED {
                    util.SendBadReqStatus(w, err)
                    return
                }                                         
                util.SendInternalServerErrorStatus(w, err)
            }   
        }
    }
    ccRegStatus := CloseContactRegStatus{
        CCRegStatus: "1",
    }
    outputJson, err := json.MarshalIndent(ccRegStatus, "", "")
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }
    fmt.Fprintf(w, "%s", outputJson)            
}

func GetNewCCByCaseAndIK(conn *pgxpool.Pool, c WbkcaseMetadata) ([]byte, error) {
    sql := 
        `select pt.name, pt.ident, pt.gender, pt.dob::text,
           pt.nationality, pt.race, pt.tel, pt.occupation,
           pt.comorbid,
           pt.address, pt.locality, pt.district, pt.state
         from wbk.wbkcase_people cp
           join wbk.peopletemp pt
             on cp.peopleident=pt.ident
         where cp.wbkcaseid=(select c.id
                          from wbk.wbkcase c
                          where c.name=$1)
           and cp.hasbeenverified=$2
           and cp.assignedtoik=$3`

    rows, err := conn.Query(context.Background(), sql, 
        c.Casename, false, c.AssignedToIk)
    if err != nil {
        return nil, err
    }

    var cco NewCloseContactsOut
    for rows.Next() {
        var name string 
        var ident string 
        var gender string 
        var dob string 
        var nationality string 
        var race string 
        var tel string 
        var occupation string  
        var comorbid []string 
        var address string 
        var locality string 
        var district string 
        var state string        

        err = rows.Scan(&name, &ident, &gender, &dob,
            &nationality, &race, &tel, &occupation, &comorbid,
            &address, &locality, &district, &state)
        if err != nil {
            return nil, err 
        }
        cc := NewCloseContactOut{
            Hasbeenreviewed: false,
            Name: name,
            Ident: ident,
            Gender: gender, 
            Dob: dob,
            Nationality: nationality,
            Race: race,
            Tel: tel,
            Occupation: occupation,
            Comorbid: comorbid,
            Address: address,
            Locality: locality,
            District: district,
            State: state,
        }
        cco.CloseContacts = append(cco.CloseContacts, cc)        
    }

    outputJson, err := json.MarshalIndent(cco, "", "")
    return outputJson, err 
}

func GetNewCCByCaseAndIKHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[GetNewCCByCaseAndIKHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var wbkcase WbkcaseMetadata
    err := json.NewDecoder(r.Body).Decode(&wbkcase)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    closeContactsJson, err := GetNewCCByCaseAndIK(db.Conn, wbkcase)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }
    fmt.Printf("%s\n", closeContactsJson)
    fmt.Fprintf(w, "%s", closeContactsJson)
}