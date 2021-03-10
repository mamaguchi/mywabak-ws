package mywabak

import (
    "net/http"
    "encoding/json"
    // "time"
    // "strconv"
    // "strings"
    // "errors"
    "fmt"
    // "log"
    "context"
    "github.com/jackc/pgx"
    "github.com/jackc/pgx/pgxpool"
    "mywabak/webservice/db"
    // "mywabak/webservice/auth"
    "mywabak/webservice/util"
)

type LocDistrict struct {
	State string 			`json:"state"`
	Districts []string 		`json:"districts"`
}

type LocLocality struct {
	State string 			`json:"state"`
	District string 		`json:"district"`
	Localities []string 	`json:"localities"`
}

type LawatanRumah struct {
	ACDName string 			`json:"acdName"`
	TarikhACD string 		`json:"tarikhACD"`
	// Locality string 		`json:"locality"`
	// District string 		`json:"district"`
	// State string 			`json:"state"`
	Bilrumahk int 			`json:"bilrumahk"`
	Bilrumahp int 			`json:"bilrumahp"`	
}

type BilRumah struct {
	Bilrumahk int 			`json:"bilrumahk"`
	Bilrumahp int 			`json:"bilrumahp"`
}

type ACD struct {
	Name string 			`json:"name"`
	Locality string 		`json:"locality"`
	District string 		`json:"district"`
	State string 			`json:"state"`
}

type ACDList struct {
	ACDs []string 				`json:"acds"`
}

type ACDActivityOneCol struct {
	ACDName string 			`json:"acdName"`
	TarikhACD string 		`json:"tarikhACD"`
	Ident string 			`json:"ident"`
	Col string 				`json:"col"`
	Val interface{}			`json:"val"`
}

type ACDActivity struct {
	ACDName string 			`json:"acdName"`
	TarikhACD string 		`json:"tarikhACD"`
	Peopleident string 		`json:"peopleident"`
	Locality string 		`json:"locality"`
	District string 		`json:"district"`
	State string 			`json:"state"`
	Kategorikes string 		`json:"kategorikes"`
	Gejala string 			`json:"gejala"`
}

type SampelOneCol struct {
	ACDName string 			`json:"acdName"`
	Ident string 			`json:"ident"`
	Col string 				`json:"col"`
	Val interface{}			`json:"val"`
}

type Sampel struct {
	ACDName string 			`json:"acdName"`
	Peopleident string 		`json:"peopleident"`
	Jenissampel string 		`json:"jenissampel"`
	Sampeltca string 		`json:"sampeltca"`
	Bildipanggil int 		`json:"bildipanggil"`
	// Sampeldiambil string 	`json:"sampeldiambil"`
	Sampeldiambil bool      `json:"sampeldiambil"`
	Sampelres string 		`json:"sampelres"`
}

type ACDPeopleOneCol struct {
	Ident string 			`json:"ident"`
	Col string 				`json:"col"`
	Val interface{}			`json:"val"`
}

type ACDHsoOneCol struct {
	ACDName string 			`json:"acdName"`
	Ident string 			`json:"ident"`
	Col string 				`json:"col"`
	Val interface{}			`json:"val"`
}

type ACDPeopleHSOandSampelIn struct {
	ACDName string 			`json:"acdName"`
	Ident string 			`json:"ident"`
}

type ACDPeopleHSOandSampelOut struct {
	Gelanghso bool 			`json:"gelanghso"`
	Annex14 bool 			`json:"annex14"`
	Pelepasan bool 			`json:"pelepasan"`
	Sampels []SampelOut		`json:"sampels"`
}

type SampelOut struct {
	Jenissampel string 		`json:"jenissampel"`
	Sampeltca string 		`json:"sampeltca"`
	Bildipanggil int 		`json:"bildipanggil"`
	Sampeldiambil bool      `json:"sampeldiambil"`
	Sampelres string 		`json:"sampelres"`
}

type ACDPeopleBasic struct {
    Ident string          `json:"ident"`
	Name string 	      `json:"name"`
	Dob string 			  `json:"dob"`
    Tel string            `json:"tel"`
    Address string        `json:"address"` 
	Comorbid string 	  `json:"comorbid"`
}

type ACDPeople struct {
    Ident string          `json:"ident"`
	Name string 	      `json:"name"`
	Dob string 			  `json:"dob"`
    Tel string            `json:"tel"`
    Address string        `json:"address"`  
    Locality string       `json:"locality"`
    District string       `json:"district"`
    State string          `json:"state"` 
    TarikhACD string      `json:"tarikhACD"`
    Kategorikes string    `json:"kategorikes"`
	Jenissampel string 	  `json:"jenissampel"`
	Gejala string 		  `json:"gejala"`
	Comorbid string 	  `json:"comorbid"`
	// Sampeltca time.Time	  `json:"sampeltca"`
	Sampeltca string	  `json:"sampeltca"`
	Bildipanggil int 	  `json:"bildipanggil"`
	// Sampeldiambil string  `json:"sampeldiambil"`
	Sampeldiambil bool    `json:"sampeldiambil"`
	Sampelres string  	  `json:"sampelres"`
	// Gelanghso string      `json:"gelanghso"`
	Gelanghso bool        `json:"gelanghso"`
	// Annex14 string        `json:"annex14"`
	Annex14 bool          `json:"annex14"`
	// Pelepasan string      `json:"pelepasan"`	
	Pelepasan bool        `json:"pelepasan"`	
	ACDName string 		  `json:"acdName"`
}

type ACDPeoples struct {
	Peoples []ACDPeople 	`json:"peoples"`
}

type BilKategoriKes struct {
	BilBergejala int 	  `json:"bilBergejala"`
	BilWargaemas int 	  `json:"bilWargaemas"`
}
// *
func GetACDList(conn *pgxpool.Pool) ([]byte, error) {
	sql :=
		`select name
		 from acd.profile`

	rows, err := conn.Query(context.Background(), sql)
	 if err != nil {
		 return nil, err
	 }

	 var acdList ACDList
	 for rows.Next() {
		 var name string 		  		
			 
		 err := rows.Scan(&name)	
		 if err != nil {		
			 return nil, err
		 }
		 
		 acdList.ACDs = append(acdList.ACDs, name)
	 }
	 outputJson, err := json.MarshalIndent(acdList, "", "\t")
	 return outputJson, err
}
// *
func GetACDListHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[GetACDListHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }      
    
    db.CheckDbConn()
    rumahJson, err := GetACDList(db.Conn)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }   
	fmt.Printf("%s\n", rumahJson)
    fmt.Fprintf(w, "%s", rumahJson)
}

// *
func UpsertACD(conn *pgxpool.Pool, acd ACD) error {
	sql :=
		`insert into acd.profile
		(
			name, locality, district, state				
		)
		values
		(
			$1, $2, $3, $4
		) 
		on conflict on constraint profile_name_key
		do 
			update set locality=$2, district=$3,
				state=$4
			where profile.name=$1`

	_, err := conn.Exec(context.Background(), sql, 
		acd.Name, acd.Locality, acd.District, acd.State)	
	if err != nil {
		return err
	}
	return nil	
}

func UpsertACDHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[UpsertACDHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var acd ACD
    err := json.NewDecoder(r.Body).Decode(&acd)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = UpsertACD(db.Conn, acd)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }   
}
// *
func GetLawatanRumah(conn *pgxpool.Pool, lr LawatanRumah) ([]byte, error) {
	sql :=
		`select bilrumahk, bilrumahp
		 from acd.house
		 where acd=$1
		   and tarikhacd=$2`

	row := conn.QueryRow(context.Background(), sql, 
		lr.ACDName, lr.TarikhACD)
	var bilrumahk int 
	var bilrumahp int 	
	err := row.Scan(&bilrumahk, &bilrumahp)
	if err != nil {
		if err == pgx.ErrNoRows { 			
			lrNotFound := BilRumah{
                Bilrumahk: 0,
				Bilrumahp: 0,
            }
            outputJson, err := json.MarshalIndent(lrNotFound, "", "\t")
			return outputJson, err
		} 
		return nil, err
	}
	bilRumah := BilRumah{				
		Bilrumahk: bilrumahk,
		Bilrumahp: bilrumahp,
	}
	outputJson, err := json.MarshalIndent(bilRumah, "", "\t")
	return outputJson, err
}

func GetLawatanRumahHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[GetLawatanRumahHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var lr LawatanRumah
    err := json.NewDecoder(r.Body).Decode(&lr)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    rumahJson, err := GetLawatanRumah(db.Conn, lr)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }   
	fmt.Printf("%s\n", rumahJson)
    fmt.Fprintf(w, "%s", rumahJson)
}
// *
func UpsertLawatanRumah(conn *pgxpool.Pool, lr LawatanRumah) error {
	var err error 

	if lr.Bilrumahp == 0 {	
		sql :=
			`insert into acd.house
			(
				acd, tarikhacd, bilrumahk
			)
			values
			(
				$1, $2, $3
			) 
			on conflict on constraint house_acd_tarikhacd_key
			do 
				update set bilrumahk=house.bilrumahk+1
				where house.acd=$1
				  and house.tarikhacd=$2`

		_, err = conn.Exec(context.Background(), sql, 
			lr.ACDName, lr.TarikhACD, lr.Bilrumahk)
	} else if lr.Bilrumahk == 0{ 
		sql :=
			`insert into acd.house
			(
				acd, tarikhacd, bilrumahp
			)
			values
			(
				$1, $2, $3
			) 
			on conflict on constraint house_acd_tarikhacd_key
			do 
				update set bilrumahp=house.bilrumahp+1
				where house.acd=$1
				  and house.tarikhacd=$2`

		_, err = conn.Exec(context.Background(), sql, 
			lr.ACDName, lr.TarikhACD, lr.Bilrumahp)
	}

	if err != nil {
		return err
	}
	return nil	
}

func UpsertLawatanRumahHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[UpsertLawatanRumahHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var lr LawatanRumah
    err := json.NewDecoder(r.Body).Decode(&lr)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = UpsertLawatanRumah(db.Conn, lr)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }   
}
// *
func GetKategoriKesSaringan(conn *pgxpool.Pool, lr LawatanRumah) ([]byte, error) {
	// sql :=
	// 	`select count(ident) as bilwargaemas, 
	// 	   (select count(ident)
	// 		 from acd.people 
	// 		 where tarikhacd=$1
	// 		   and locality=$2
	// 		   and district=$3
	// 		   and state=$4
	// 		   and kategorikes='Bergejala') as bilbergejala
	// 	 from acd.people 
	// 	 where tarikhacd=$1
	// 	   and locality=$2
	// 	   and district=$3
	// 	   and state=$4
	// 	   and kategorikes='Warga Emas'`

	sql :=
		`select count(peopleident) as bilwargaemas, 
		   (select count(peopleident)
			 from acd.activity 
			 where tarikhacd=$1
			   and acd=$2			   
			   and kategorikes='Bergejala') as bilbergejala
		 from acd.activity 
		 where tarikhacd=$1
		   and acd=$2		   
		   and kategorikes='Warga Emas Perlu Disaring'`

	row := conn.QueryRow(context.Background(), sql, 
		lr.TarikhACD, lr.ACDName)
	var bilWargaemas int 
	var bilBergejala int 	
	err := row.Scan(&bilWargaemas, &bilBergejala)
	if err != nil {
		if err == pgx.ErrNoRows { 			
			bilKategoriKes := BilKategoriKes{				
				BilBergejala: 0,
				BilWargaemas: 0,
			}
            outputJson, err := json.MarshalIndent(bilKategoriKes, "", "\t")
			return outputJson, err
		} 
		return nil, err
	}
	bilKategoriKes := BilKategoriKes{				
		BilBergejala: bilBergejala,
		BilWargaemas: bilWargaemas,
	}
	outputJson, err := json.MarshalIndent(bilKategoriKes, "", "\t")
	return outputJson, err
}

func GetKategoriKesSaringanHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[GetKategoriKesSaringanHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var lr LawatanRumah
    err := json.NewDecoder(r.Body).Decode(&lr)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    saringanJson, err := GetKategoriKesSaringan(db.Conn, lr)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }   
	fmt.Printf("%s\n", saringanJson)
    fmt.Fprintf(w, "%s", saringanJson)
}

func GetDistricts(conn *pgxpool.Pool, d LocDistrict) ([]byte, error) {			
	sql :=
		`select d.district	 
		 from acd.acddistrict d		      
		 where state=$1`

	rows, err := conn.Query(context.Background(), sql, 
		d.State)
	if err != nil {
		return nil, err
	}

	var locDistrict LocDistrict
	for rows.Next() {	
		var district string 
	 
		err := rows.Scan(&district)				
		if err != nil {					
			return nil, err
		}

		locDistrict.Districts = append(locDistrict.Districts, district)		
	}
	outputJson, err := json.MarshalIndent(locDistrict, "", "\t")
	return outputJson, err	
}

func GetDistrictsHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[GetDistrictsHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var locDistrict LocDistrict
    err := json.NewDecoder(r.Body).Decode(&locDistrict)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    locDistrictJson, err := GetDistricts(db.Conn, locDistrict)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }
    fmt.Printf("%s\n", locDistrictJson)
    fmt.Fprintf(w, "%s", locDistrictJson)
}

func GetLocalities(conn *pgxpool.Pool, l LocLocality) ([]byte, error) {			
	sql :=
		`select l.locality	 
		 from acd.acdlocality l		      
		 where state=$1
		   and district=$2`

	rows, err := conn.Query(context.Background(), sql, 
		l.State, l.District)
	if err != nil {
		return nil, err
	}

	var locLocality = LocLocality{
		State: l.State,
		District: l.District,
	}
	for rows.Next() {	
		var locality string 
	 
		err := rows.Scan(&locality)				
		if err != nil {					
			return nil, err
		}

		locLocality.Localities = append(locLocality.Localities, locality)		
	}
	outputJson, err := json.MarshalIndent(locLocality, "", "\t")
	return outputJson, err	
}

func GetLocalitiesHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[GetLocalityHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var locLocality LocLocality
    err := json.NewDecoder(r.Body).Decode(&locLocality)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    locLocalityJson, err := GetLocalities(db.Conn, locLocality)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }
    fmt.Printf("%s\n", locLocalityJson)
    fmt.Fprintf(w, "%s", locLocalityJson)
}
//*
func AddSaringan(conn *pgxpool.Pool, ap ACDPeople) error {
	// sql :=
	// 	`insert into acd.people
	// 	(
	// 		ident, name, tel, address, locality,
	// 		district, state, tarikhacd, kategorikes, jenissampel
	// 	)
	// 	 values
	// 	(
	// 		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10
	// 	)`

	// sql1 :=
	// 	`insert into acd.people
	// 	(
	// 		ident, name, dob, tel, address, locality,
	// 		district, state, comorbid
	// 	)
	// 	 values
	// 	(
	// 		$1, $2, $3, $4, $5, $6, $7, $8, $9
	// 	)`

	sql1 :=
		`insert into acd.people
		(
			ident, name, dob, tel, address, comorbid
		)
		values
		(
			$1, $2, $3, $4, $5, $6
		)   
		on conflict on constraint people_ident_key
		do 
		    update set name=$2, dob=$3, tel=$4,
			  address=$5, comorbid=$6
			where people.ident=$1`

	_, err := conn.Exec(context.Background(), sql1, 
		ap.Ident, ap.Name, ap.Dob, ap.Tel, ap.Address, ap.Comorbid)
	if err != nil {
		return err
	}

	sql2 :=
		`insert into acd.activity
		(
			acd, peopleident, tarikhacd, kategorikes, gejala
		)
		 values
		(
			$1, $2, $3, $4, $5
		)
		on conflict on constraint activity_acd_peopleident_tarikhacd_key
		do 
		    update set kategorikes=$4, gejala=$5
			where activity.acd=$1
			  and activity.peopleident=$2
			  and activity.tarikhacd=$3`

	_, err = conn.Exec(context.Background(), sql2, 
		ap.ACDName, ap.Ident, ap.TarikhACD,  
		ap.Kategorikes, ap.Gejala)
    if err != nil {
        return err
    }
    return nil
}

func AddSaringanHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[AddSaringanHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var ap ACDPeople
    err := json.NewDecoder(r.Body).Decode(&ap)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = AddSaringan(db.Conn, ap)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }   
}

func GetSaringanBasic(conn *pgxpool.Pool, ident string) ([]byte, error) {			
	sql :=
		`select p.name, p.dob::text, 
		   p.tel, p.address,
		   coalesce(p.comorbid, '') as comorbid		 
		 from acd.people p		      
		 where ident=$1`

	row := conn.QueryRow(context.Background(), sql, 
		ident)

	var name string 
	var dob string
	var tel string 
	var address string 
	var comorbid string 
	err := row.Scan(&name, &dob, &tel, &address, &comorbid)				
	if err != nil {
		// People Ident doesn't exist, 
		// so can sign up a new account.
		if err == pgx.ErrNoRows { 			
			peopleNotFound := ACDPeople{
				Ident: "NOTFOUND",
			}
			outputJson, err := json.MarshalIndent(peopleNotFound, "", "\t")
			return outputJson, err
		} 
		return nil, err
	}

	acdPeople := ACDPeopleBasic{				
		Ident: ident,
		Name: name,
		Dob: dob,
		Tel: tel,
		Address: address,
		Comorbid: comorbid,			
	}
	outputJson, err := json.MarshalIndent(acdPeople, "", "\t")
	return outputJson, err	
}

func GetSaringanBasicHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[GetSaringanBasicHandler] request received")    

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
    saringanBasicJson, err := GetSaringanBasic(db.Conn, identity.Ident)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }
    fmt.Printf("%s\n", saringanBasicJson)
    fmt.Fprintf(w, "%s", saringanBasicJson)
}
// *    
func GetSaringan(conn *pgxpool.Pool, lr LawatanRumah) ([]byte, error) {
	// sql :=
	// 	`select p.ident, p.name, p.dob::text, 
	// 	 p.tel, p.address,
	// 	 coalesce(p.comorbid, '') as comorbid,
	// 	 p.gelanghso, 
	// 	 p.annex14,
	// 	 p.pelepasan,
	// 	 coalesce(a.kategorikes, '') as kategorikes,
	// 	 coalesce(a.gejala, '') as gejala,
	// 	 coalesce(s.jenissampel, '') as jenissampel,
		//  coalesce(s.sampeltca::text, '') as sampeltca,
		//  coalesce(s.sampeldiambil, false) as sampeldiambil, 
		//  coalesce(s.bildipanggil, 0) as bildipanggil,
		//  coalesce(s.sampelres, '') as sampelres
	// 	 from acd.acdactivity a
	// 	   join acd.people p
	// 	     on a.peopleident = p.ident	
	// 	   left join acd.sampel s
	// 	     on p.ident = s.peopleident	   
	// 	 where a.tarikhacd::text=$1
	// 	 and a.locality=$2
	// 	 and a.district=$3
	// 	 and a.state=$4`

	sql :=
		`select p.ident, p.name, p.dob::text, 
		 p.tel, p.address,
		 coalesce(p.comorbid, '') as comorbid,		 
		 coalesce(a.tarikhacd::text, '') as tarikhacd,
		 coalesce(a.kategorikes, '') as kategorikes,
		 coalesce(a.gejala, '') as gejala		 
		 from acd.profile profile		   	
		   left join acd.activity a
		     on profile.name = a.acd	
		   left join acd.people p
		     on a.peopleident = p.ident   
		 where profile.name=$1
		   and a.tarikhacd::text ilike $2`

	rows, err := conn.Query(context.Background(), sql, 
		lr.ACDName, lr.TarikhACD)
	if err != nil {
		return nil, err
	}
	var acdPeoples ACDPeoples

	for rows.Next() {
		var ident string 
		var name string 
		var dob string
		var tel string 
		var address string 
		var tarikhACD string
		var comorbid string 		 
		var	kategorikes string
		var gejala string 		
			
		err := rows.Scan(&ident, &name, &dob, &tel, &address, 
			&comorbid, &tarikhACD, &kategorikes, &gejala)	
		if err != nil {		
			return nil, err
		}
		
		acdPeople := ACDPeople{				
			Ident: ident,
			Name: name,
			Dob: dob,
			Tel: tel,
			Address: address,
			Comorbid: comorbid,	
			TarikhACD: tarikhACD,			
			Kategorikes: kategorikes,
			Gejala: gejala,			
		}
		acdPeoples.Peoples = append(acdPeoples.Peoples,
			acdPeople)
	}
	outputJson, err := json.MarshalIndent(acdPeoples, "", "\t")
	return outputJson, err
}

func GetSaringanHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[GetSaringanHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var lr LawatanRumah
    err := json.NewDecoder(r.Body).Decode(&lr)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    saringanJson, err := GetSaringan(db.Conn, lr)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }   
	fmt.Printf("%s\n", saringanJson)
    fmt.Fprintf(w, "%s", saringanJson)
}

func GetHSOandSample(conn *pgxpool.Pool, aphsi ACDPeopleHSOandSampelIn) ([]byte, error) {		
	// HSO
	sqlHSO :=
		`select h.gelanghso, h.annex14, h.pelepasan		 
		 from acd.hso h		   			     
		 where h.acd=$1
		   and h.peopleident=$2`

	row := conn.QueryRow(context.Background(), sqlHSO, 
		aphsi.ACDName, aphsi.Ident)	

	var gelanghso bool
	var annex14 bool 
	var pelepasan bool
	err := row.Scan(&gelanghso, &annex14, &pelepasan)	
	if err != nil {	
		if err == pgx.ErrNoRows { 			
		//Do nothing
		} else {
			return nil, err
		}	
	}	
	aphso := ACDPeopleHSOandSampelOut{
		Gelanghso: gelanghso,
		Annex14: annex14,
		Pelepasan: pelepasan,
	}

	// Sample
	sqlSample :=
		`select s.jenissampel, 
		   coalesce(s.sampeltca::text, '') as sampeltca,
		   coalesce(s.bildipanggil, 0) as bildipanggil,
		   coalesce(s.sampeldiambil, false) as sampeldiambil,
		   coalesce(s.sampelres, '') as sampelres
		 from acd.sampel s		   			     
		 where s.acd=$1
		   and s.peopleident=$2`

	rows, err := conn.Query(context.Background(), sqlSample, 
		aphsi.ACDName, aphsi.Ident)	
	if err != nil {
		return nil, err
	}

	for rows.Next() {	
		var jenissampel string 
		var sampeltca string 
		var bildipanggil int 
		var sampeldiambil bool 
		var sampelres string 

		err = rows.Scan(&jenissampel, &sampeltca, &bildipanggil,
			&sampeldiambil, &sampelres)	
		if err != nil {				
			return nil, err
		}

		// OUTPUT
		sampel := SampelOut{						
			Jenissampel: jenissampel,
			Sampeltca: sampeltca,
			Bildipanggil: bildipanggil,
			Sampeldiambil: sampeldiambil,
			Sampelres: sampelres,		
		}
		aphso.Sampels = append(aphso.Sampels, sampel)
	}
	outputJson, err := json.MarshalIndent(aphso, "", "")
	return outputJson, err	
}

func GetHSOandSampleHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[GetHSOandSampleHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var aphsi ACDPeopleHSOandSampelIn
    err := json.NewDecoder(r.Body).Decode(&aphsi)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    acdHSOandSampelJson, err := GetHSOandSample(db.Conn, aphsi)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }   
	fmt.Printf("%s\n", acdHSOandSampelJson)
    fmt.Fprintf(w, "%s", acdHSOandSampelJson)
}

// *	
func UpdateACDPeople(conn *pgxpool.Pool, ap ACDPeople) error {
	// sql := 
	// 	`update acd.people
	// 	   set name=$1, dob=$2, tel=$3, address=$4, locality=$5,
	// 	     district=$6, state=$7, comorbid=$8, gelanghso=$9,
	// 		 annex14=$10, pelepasan=$11
	// 	   where ident=$12`	

	sql := 
		`update acd.people
		   set name=$1, dob=$2, tel=$3, address=$4, locality=$5,
		     district=$6, state=$7, comorbid=$8
		 where ident=$9`	

	_, err := conn.Exec(context.Background(), sql,
		ap.Name, ap.Dob, ap.Tel, ap.Address, ap.Locality, 
		ap.District, ap.State, ap.Comorbid, ap.Ident)                         
	if err != nil {
		return err
	   }
	return nil
}

func UpdateACDPeopleHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[UpdateACDPeopleHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var ap ACDPeople
    err := json.NewDecoder(r.Body).Decode(&ap)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = UpdateACDPeople(db.Conn, ap)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }   
}
// *
func UpdateACDHso(conn *pgxpool.Pool, ap ACDPeople) error {
	sql := 
		`update acd.people
		   set gelanghso=$1, annex14=$2, pelepasan=$3
		 where acd=$4 
		   and ident=$5`	

	_, err := conn.Exec(context.Background(), sql,
		ap.Gelanghso, ap.Annex14, ap.Pelepasan, 
		ap.ACDName, ap.Ident)                         
	if err != nil {
		return err
	   }
	return nil
}

func UpdateACDHsoHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[UpdateACDHsoHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var ap ACDPeople
    err := json.NewDecoder(r.Body).Decode(&ap)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = UpdateACDHso(db.Conn, ap)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }   
}

/* acd.people - Columns */
// name, tel, address, comorbid, gelanghso, annex14, pelepasan 
func UpdateACDPeopleOneCol(conn *pgxpool.Pool, apoc ACDPeopleOneCol) error {	

	sql := 
		`update acd.people
		   set %s=$1
		   where ident=$2`
	sql = fmt.Sprintf(sql, apoc.Col)

	var err error	
	val := apoc.Val.(string)
	_, err = conn.Exec(context.Background(), sql,
		val, apoc.Ident) 	                       
	if err != nil {
		return err
	   }
	return nil
}

func UpdateACDPeopleOneColHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[UpdateACDPeopleOneColHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var apoc ACDPeopleOneCol
    err := json.NewDecoder(r.Body).Decode(&apoc)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = UpdateACDPeopleOneCol(db.Conn, apoc)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }   
}

func UpdateACDHsoOneCol(conn *pgxpool.Pool, ahoc ACDHsoOneCol) error {	

	sql := 
		`update acd.hso
		   set %s=$1
		   where acd=$2
		     and peopleident=$3`
	sql = fmt.Sprintf(sql, ahoc.Col)

	var err error
	val := ahoc.Val.(bool)
	_, err = conn.Exec(context.Background(), sql,
		val, ahoc.ACDName, ahoc.Ident) 		                       
	if err != nil {
		return err
	   }
	return nil
}

func UpdateACDHsoOneColHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[UpdateACDHsoOneColHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var ahoc ACDHsoOneCol
    err := json.NewDecoder(r.Body).Decode(&ahoc)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = UpdateACDHsoOneCol(db.Conn, ahoc)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }   
}
// *	
func UpdateACDActivity(conn *pgxpool.Pool, aa ACDActivity) error {
	// sql := 
	// 	`update acd.acdactivity
	// 	   set locality=$1, district=$2, state=$3, kategorikes=$4,
	// 	     gejala=$5
	// 	   where tarikhacd=$6
	// 	     and peopleident=$7`

	sql := 
		`update acd.activity
		   set kategorikes=$1, gejala=$2
		   where acd=$3
		     and peopleident=$4
		     and tarikhacd=$5`

	_, err := conn.Exec(context.Background(), sql,
		aa.Kategorikes, aa.Gejala, 
		aa.ACDName, aa.Peopleident, aa.TarikhACD)                         
	if err != nil {
		return err
	   }
	return nil
}

func UpdateACDActivityHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[UpdateACDActivityHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var aa ACDActivity
    err := json.NewDecoder(r.Body).Decode(&aa)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = UpdateACDActivity(db.Conn, aa)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }   
}
// *
/* acd.acdactivity - Columns */
// kategorikes, gejala
func UpdateACDactivityOneCol(conn *pgxpool.Pool, aaoc ACDActivityOneCol) error {	

	sql := 
		`update acd.activity
		   set %s=$1
		   where acd=$2
		     and peopleident=$3
			 and tarikhacd=$4`
	sql = fmt.Sprintf(sql, aaoc.Col)

	val := aaoc.Val.(string)
	_, err := conn.Exec(context.Background(), sql,
		val, aaoc.ACDName, aaoc.Ident, aaoc.TarikhACD) 		                       
	if err != nil {
		return err
	   }
	return nil
}

func UpdateACDactivityOneColHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[UpdateACDactivityOneColHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var aaoc ACDActivityOneCol
    err := json.NewDecoder(r.Body).Decode(&aaoc)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = UpdateACDactivityOneCol(db.Conn, aaoc)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }   
}
// *
func UpdateSampel(conn *pgxpool.Pool, s Sampel) error {
	// sql := 
	// 	`update acd.acdactivity
	// 	   set jenissampel=$1, sampeltca=$2, sampeldiambil=$3, 
	// 	   bildipanggil=$4, sampelres=$5
	// 	 where peopleident=$6`

	sql := 
		`update acd.sampel
		   set jenissampel=$1, sampeltca=$2, sampeldiambil=$3, 
		   bildipanggil=$4, sampelres=$5
		 where acd=$6
		   and peopleident=$7`

	_, err := conn.Exec(context.Background(), sql,
		s.Jenissampel, s.Sampeltca, s.Sampeldiambil,
		s.Bildipanggil, s.Sampelres, s.ACDName, s.Peopleident)                         
	if err != nil {
		return err
	   }
	return nil
}

func UpdateSampelHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[UpdateSampelHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var s Sampel
    err := json.NewDecoder(r.Body).Decode(&s)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = UpdateSampel(db.Conn, s)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }   
}
// *
/* acd.sampel - Columns */
// jenissampel, sampeltca, bildipanggil, sampeldiambil, sampelres
func UpdateSampelOneCol(conn *pgxpool.Pool, soc SampelOneCol) error {	

	sql := 
		`update acd.sampel
		   set %s=$1
		   where acd=$2
		     and peopleident=$3`
	sql = fmt.Sprintf(sql, soc.Col)

	var err error
	if soc.Col == "sampeldiambil" {
		val := soc.Val.(bool)
		_, err = conn.Exec(context.Background(), sql,
		val, soc.ACDName, soc.Ident) 
	} else if soc.Col == "bildipanggil" {
		val := int(soc.Val.(float64))
		_, err = conn.Exec(context.Background(), sql,
		val, soc.ACDName, soc.Ident) 
	} else {
		val := soc.Val.(string)
		_, err = conn.Exec(context.Background(), sql,
		val, soc.ACDName, soc.Ident) 
	}
	                       
	if err != nil {
		return err
	   }
	return nil
}

func UpdateSampelOneColHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[UpdateSampelOneColHandler] request received")    

    // VERIFY AUTH TOKEN
    // authToken := strings.Split(r.Header.Get("Authorization"), " ")[1]
    // if !auth.VerifyTokenHMAC(authToken) {
    //     util.SendUnauthorizedStatus(w)
    //     return
    // }   

    var soc SampelOneCol
    err := json.NewDecoder(r.Body).Decode(&soc)
    if err != nil {
        util.SendInternalServerErrorStatus(w, err)
        return
    }
    
    db.CheckDbConn()
    err = UpdateSampelOneCol(db.Conn, soc)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }   
}




