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

type HealthStaff struct {
    Name string             `json:"name"`
    Ident string            `json:"ident"`
}

type HealthStaffByCase struct {
    Name string             `json:"name"`
    Ident string            `json:"ident"`
    Assigned bool           `json:"assigned"`
}

type StaffsByOrgIn struct {
    HealthOrg string        `json:"healthOrg"`
}

type StaffsByOrgOut struct {
    Staffs []HealthStaff    `json:"staffs"`
}

type AssignedStaffsByCaseAndOrgIn struct {
    HealthOrg string        `json:"healthOrg"`
    Casename string         `json:"casename"`
}

// [DEPRECATED]
// type AssignedStaffsByCaseAndOrgOut struct {
//     Staffs []HealthStaff    `json:"staffs"`
// }

type AssignedStaffsByOrgOut struct {    
    Staffs map[string]map[string][]HealthStaff `json:"staffs"`
}

type AssignedStaffsByCaseAndOrgOut struct {
    Begindt string       `json:"begindt"`
    Description string   `json:"description"`
    District string      `json:"district"`
    State string         `json:"state"`
    Staffs map[string]map[string][]HealthStaffByCase `json:"staffs"`
}

type AssignedStaffsByCaseIn struct {
    Casename string         `json:"casename"`
    Begindt string          `json:"begindt"`
    Enddt string            `json:"enddt"`
    Description string      `json:"description"`
    State string            `json:"state"`
    District string         `json:"district"`
    AssignedStaffs []string `json:"assignedStaffs"`
}

type AddWbkcaseStatus struct {
    AddStatus string          `json:"addStatus"`
}

type UpdateAssignedStaffsByCaseStatus struct {
    UpdateStatus string      `json:"updateStatus"`
}

type CloseContactSearchStatus struct {
    CCSearchStatus string      `json:"ccSearchStatus"`
}

type CasesListByDistrictIn struct {
    State string            `json:"state"`
    District string         `json:"district"`
}

type CasesListByDistrictOut struct {
    Cases []CaseOut         `json:"cases"`
    GetStatus string        `json:"getStatus"`
}

type CaseOut struct {
    Casename string         `json:"casename"`
    BeginDt string          `json:"beginDt"`
    NumPosCase int          `json:"numPosCase"`
    ResultDt string         `json:"resultDt"`
    NumCC int               `json:"numCC"`
    Clustername string      `json:"clustername"`
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
// select staff.name, case when 
// (select s.ident from wbk.staff s left join wbk.wbkcase 
// c on s.ident=any(c.assignedstaffs) where c.name='' 
// and s.organization='PKD Maran' and s.ident=staff.ident) 
// is not null then 'yes' else 'no' end as TICK from wbk.staff;

func GetStaffsByOrg(conn *pgxpool.Pool, si StaffsByOrgIn) ([]byte, error) {
    sql := 
        `select s.name, s.ident, s.position, s.unit
         from wbk.staff s
         where s.organization=$1`    

    rows, err := conn.Query(context.Background(), sql, 
        si.HealthOrg)  
    if err != nil {
        return nil, err 
    }
   
    n := make(map[string]map[string][]HealthStaff)
    for rows.Next() {
        var name string 
        var ident string 
        var position string 
        var unit string 

        err = rows.Scan(&name, &ident, &position, &unit)
        if err != nil {
            return nil, err 
        }

        staff := HealthStaff{
            Name: name,
            Ident: ident,
        }
        if n[position]==nil {
            m := make(map[string][]HealthStaff)
            n[position] = m 
            n[position][unit] = append(n[position][unit], staff)
        } else {
            n[position][unit] = append(n[position][unit], staff)
        }
    } 
    output := AssignedStaffsByOrgOut{
        Staffs: n,
    }
    outputJson, err := json.MarshalIndent(output, "", "")
    return outputJson, err
}

func GetStaffsByOrgHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[GetStaffsByOrgHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var si StaffsByOrgIn
    err := json.NewDecoder(r.Body).Decode(&si)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    healthStaffsJson, err := GetStaffsByOrg(db.Conn, si)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }
    fmt.Printf("%s\n", healthStaffsJson)
    fmt.Fprintf(w, "%s", healthStaffsJson)
}

func AddWbkcase(conn *pgxpool.Pool, asi AssignedStaffsByCaseIn) error {
    var err error
    if asi.Enddt == "" {
        sql := 
        `insert into wbk.wbkcase
        (
            name, description, state, district, 
            assignedstaffs, begindt
        )
        values
        (
            $1, $2, $3, $4, $5, $6
        )`                     

    _, err = conn.Exec(context.Background(), sql, 
        asi.Casename, asi.Description,
        asi.State, asi.District, asi.AssignedStaffs,
        asi.Begindt)
    } else {
        sql := 
            `insert into wbk.wbkcase
            (
                name, description, state, district, 
                assignedstaffs, begindt, enddt
            )
            values
            (
                $1, $2, $3, $4, $5, $6, $7
            )`                     

        _, err = conn.Exec(context.Background(), sql, 
            asi.Casename, asi.Description,
            asi.State, asi.District, asi.AssignedStaffs,
            asi.Begindt, asi.Enddt)
    }    
    if err != nil {
        return err
    }
    return nil
}

func AddWbkcaseHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[AddWbkcaseHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var asi AssignedStaffsByCaseIn
    err := json.NewDecoder(r.Body).Decode(&asi)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = AddWbkcase(db.Conn, asi)
    if err != nil {                                        
        if pgerr, ok := err.(*pgconn.PgError); ok {
			if pgerr.ConstraintName == "wbkcase_name_key" {
				fmt.Fprintf(os.Stderr, "Unable to insert a new entry into wbkcase, because a same casename already exists: %v\n", pgerr)
                addStatus := AddWbkcaseStatus{
                    AddStatus: "CASENAMEEXISTS",
                }
                outputJson, err := json.MarshalIndent(addStatus, "", "")
                if err != nil {
                    util.SendInternalServerErrorStatus(w, err)
                }
                fmt.Fprintf(w, "%s", outputJson)
                return
			} else {
				fmt.Fprintf(os.Stderr, "Unexpected postgres error trying to insert a wbkcase entry: %v\n", pgerr)
                util.SendInternalServerErrorStatus(w, pgerr)
                return
			}
		} else {
			fmt.Fprintf(os.Stderr, "Unexpected error trying to insert a wbkcase entry: %v\n", err)
            util.SendInternalServerErrorStatus(w, err)
            return
		}
    }   
    
    addStatus := AddWbkcaseStatus{
        AddStatus: "1",
    }
    outputJson, err := json.MarshalIndent(addStatus, "", "")
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }
    fmt.Fprintf(w, "%s", outputJson) 
}

/* [DEPRECATED]
func GetAssignedStaffsByOrgAndCase_OLD(conn *pgxpool.Pool, asi AssignedStaffsByCaseAndOrgIn) ([]byte, error) {
    sql := 
        `select s.name, s.ident
         from wbk.staff s
           join wbk.wbkcase c
             on s.ident=any(c.assignedstaffs)
         where c.name=$1
           and s.organization=$2`

    rows, err := conn.Query(context.Background(), sql, 
        asi.Casename, asi.HealthOrg)
    if err != nil {
        return nil, err 
    }

    var aso AssignedStaffsByCaseAndOrgOut
    for rows.Next() {
        var name string 
        var ident string 

        err = rows.Scan(&name, &ident)
        if err != nil {
            return nil, err 
        }
        staff := HealthStaff{
            Name: name,
            Ident: ident,
        }
        aso.Staffs = append(aso.Staffs, staff)
    }
    outputJson, err := json.MarshalIndent(aso, "", "")
    return outputJson, err
}
*/

func GetAssignedStaffsByOrgAndCase(conn *pgxpool.Pool, asi AssignedStaffsByCaseAndOrgIn) ([]byte, error) {       
    sql1 := 
        `select s.name, s.ident, s.position, s.unit,
           case 
             when (select staff.ident from wbk.staff
                     left join wbk.wbkcase
                       on staff.ident=any(wbkcase.assignedstaffs)
                   where wbkcase.name=$1
                     and staff.ident=s.ident) is not null then true
           else
             false 
           end as assigned
         from wbk.staff s
         where s.organization=$2`    

    sql2 := 
        `select begindt::text, description, district, state
         from wbk.wbkcase
         where wbkcase.name=$1`

    b := &pgx.Batch{}
    b.Queue(sql1, asi.Casename, asi.HealthOrg)
    b.Queue(sql2, asi.Casename)

    var br pgx.BatchResults
    br = conn.SendBatch(context.Background(), b) 

    // Run sql1
    rows, err := br.Query()
    if err != nil {
        return nil, err 
    } 
    n := make(map[string]map[string][]HealthStaffByCase)
    for rows.Next() {
        var name string 
        var ident string 
        var position string 
        var unit string 
        var assigned bool

        err = rows.Scan(&name, &ident, &position, &unit, &assigned)
        if err != nil {
            return nil, err 
        }

        staff := HealthStaffByCase{
            Name: name,
            Ident: ident,
            Assigned: assigned,
        }
        if n[position]==nil {
            m := make(map[string][]HealthStaffByCase)
            n[position] = m 
            n[position][unit] = append(n[position][unit], staff)
        } else {
            n[position][unit] = append(n[position][unit], staff)
        }
    } 
    
    // Run sql2
    row := br.QueryRow()
    var begindt string
    var description string 
    var district string 
    var state string 
    err = row.Scan(&begindt, &description, &district, &state)
    if err != nil {
        return nil, err 
    }

    // Output
    err = br.Close()
    if err != nil { 
        return nil, err
    }
    output := AssignedStaffsByCaseAndOrgOut{
        Begindt: begindt,
        Description: description,
        District: district,
        State: state,
        Staffs: n,
    }
    outputJson, err := json.MarshalIndent(output, "", "")
    return outputJson, err
}

func GetAssignedStaffsByOrgAndCaseHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[GetAssignedStaffsByOrgAndCaseHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var asi AssignedStaffsByCaseAndOrgIn
    err := json.NewDecoder(r.Body).Decode(&asi)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    healthStaffsJson, err := GetAssignedStaffsByOrgAndCase(db.Conn, asi)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }
    fmt.Printf("%s\n", healthStaffsJson)
    fmt.Fprintf(w, "%s", healthStaffsJson)
}

func UpdateAssignedStaffsByCase(conn *pgxpool.Pool, asi AssignedStaffsByCaseIn) error {
    // sql1 := 
    //     `update wbk.wbkcase
    //         set assignedstaffs=(assignedstaffs || $1),
    //           description=$2,
    //           states=(states || $3),
    //           districts=(districts || $4) 
    //         where name=$5`       
         
    // sql2 :=
    //     `update wbk.wbkcase
    //        set assignedstaffs=(select array_agg(distinct e)
    //                             from unnest(assignedstaffs) e)
    //      where name=$1`

    b := &pgx.Batch{}

    if asi.Enddt == "" {
        sql1 := 
            `update wbk.wbkcase
                set assignedstaffs=$1,
                  begindt=$2,
                  enddt=null,
                  description=$3,
                  state=$4,
                  district=$5
                where name=$6`     
                
        b.Queue(sql1, asi.AssignedStaffs, 
            asi.Begindt, asi.Description, 
            asi.State, asi.District, asi.Casename)
    } else {
        sql1 := 
        `update wbk.wbkcase
            set assignedstaffs=$1,
              begindt=$2,
              enddt=$3,
              description=$4,
              state=$5,
              district=$6
            where name=$7`

        b.Queue(sql1, asi.AssignedStaffs, 
            asi.Begindt, asi.Enddt, asi.Description, 
            asi.State, asi.District, asi.Casename)
    }        
    // b.Queue(sql2, asi.Casename)

    var br pgx.BatchResults
    br = conn.SendBatch(context.Background(), b)    
    
    for i:=0; i<b.Len(); i++ {
        _, err := br.Exec()
        if err != nil {
            return err 
        }
    }

    err := br.Close()
    if err != nil { 
        return err
    }
    return nil
}

func UpdateAssignedStaffsByCaseHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[UpdateAssignedStaffsByCaseHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var asi AssignedStaffsByCaseIn
    err := json.NewDecoder(r.Body).Decode(&asi)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = UpdateAssignedStaffsByCase(db.Conn, asi)
    if err != nil {                      
        util.SendInternalServerErrorStatus(w, err)        
    }    

    // updateStatus := UpdateAssignedStaffsByCaseStatus{
    //     UpdateStatus: "1",
    // }
    // outputJson, err := json.MarshalIndent(updateStatus, "", "")
    // if err != nil {        
    //     util.SendInternalServerErrorStatus(w, err)
    //     return 
    // }
    // fmt.Fprintf(w, "%s", outputJson)  
}

func GetCasesListByDistrict(conn *pgxpool.Pool, clbd CasesListByDistrictIn) ([]byte, error) {
    sql :=
        `select wbkcase.name, wbkcase.begindt::text, s1.numposcase, 
           coalesce(s2.resultdt::text, '') as resultdt, 
           s3.numcc, 
           coalesce(s4.clustername, '') as clustername
        from wbk.wbkcase wbkcase
          left join lateral
          (select count(p.ident) as numposcase
             from wbk.wbkcase c
               join wbk.wbkcase_people cp
                 on c.id = cp.wbkcaseid
               join wbk.people p
                 on cp.peopleident = p.ident
               join wbk.sampling s
                 on p.ident = s.peopleident
             where c.name = wbkcase.name
               and s.wbkcaseid = c.id
               and s.samplingres::text = 'Positive'
               and s.samplingtype::text = 'RT-PCR') s1 on true
          left join lateral
          (select s.resultdt as resultdt
             from wbk.wbkcase c
               join wbk.wbkcase_people cp
                 on c.id = cp.wbkcaseid
               join wbk.people p
                 on cp.peopleident = p.ident
               join wbk.sampling s
                 on p.ident = s.peopleident
             where c.name = wbkcase.name
               and s.wbkcaseid = c.id
               and s.samplingres::text = 'Positive'
               and s.samplingtype::text = 'RT-PCR'
             order by resultdt desc
               limit 1) s2 on true
          left join lateral
          (select count(p.ident) as numcc
             from wbk.wbkcase c
               join wbk.wbkcase_people cp
                 on c.id = cp.wbkcaseid
               join wbk.people p
                 on cp.peopleident = p.ident
               left join wbk.sampling s
                 on p.ident = s.peopleident
             where c.name = wbkcase.name
               and s.wbkcaseid = c.id
               and (select samplingres 
                      from wbk.sampling
                      where sampling.peopleident = cp.contactto
                        and sampling.samplingres::text = 'Positive'
                        and sampling.samplingtype::text = 'RT-PCR') is not null) s3 on true
          left join lateral
          (select c.name as clustername
             from wbk.cluster c
             where c.id = wbkcase.clusterid) s4 on true
          where state=$1
            and district=$2`    

    rows, err := conn.Query(context.Background(), sql, 
        clbd.State, clbd.District)
    if err != nil {
        return nil, err
    }

    var cases CasesListByDistrictOut
    for rows.Next() {
        var casename string 
        var begindt string 
        var numposcase int 
        var resultdt string 
        var numcc int 
        var clustername string 

        err = rows.Scan(&casename, &begindt, &numposcase,
            &resultdt, &numcc, &clustername)
        if err != nil {
            return nil, err 
        }
        wbkcase := CaseOut{
            Casename: casename,
            BeginDt: begindt,
            NumPosCase: numposcase,
            ResultDt: resultdt,
            NumCC: numcc,
            Clustername: clustername, 
        }
        cases.Cases = append(cases.Cases, wbkcase)
    }

    if cases.Cases == nil || len(cases.Cases)==0 {
        cases.GetStatus = "NOROWS"
    }
    outputJson, err := json.MarshalIndent(cases, "", "")
    return outputJson, err
}

func GetCasesListByDistrictHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[GetCasesListByDistrictHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var clbd CasesListByDistrictIn
    err := json.NewDecoder(r.Body).Decode(&clbd)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    casesJson, err := GetCasesListByDistrict(db.Conn, clbd)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }
    fmt.Printf("%s\n", casesJson)
    fmt.Fprintf(w, "%s", casesJson)
}