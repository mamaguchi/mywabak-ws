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

type LawatanRumah struct {
	TarikhACD string 		`json:"tarikhACD"`
	Locality string 		`json:"locality"`
	District string 		`json:"district"`
	State string 			`json:"state"`
	Bilrumahk int 			`json:"bilrumahk"`
	Bilrumahp int 			`json:"bilrumahp"`
}

type ACDActivity struct {
	TarikhACD string 		`json:"tarikhACD"`
	Peopleident string 		`json:"peopleident"`
	Locality string 		`json:"locality"`
	District string 		`json:"district"`
	State string 			`json:"state"`
	Kategorikes string 		`json:"kategorikes"`
	Gejala string 			`json:"gejala"`
}

type Sampel struct {
	Peopleident string 		`json:"peopleident"`
	Jenissampel string 		`json:"jenissampel"`
	Sampeltca string 		`json:"sampeltca"`
	Sampeldiambil string 	`json:"sampeldiambil"`
	Bildipanggil int 		`json:"bildipanggil"`
	Sampelres string 		`json:"sampelres"`
}

type ACDPeople struct {
    Ident string          `json:"ident"`
	Name string 	      `json:"name"`
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
	Sampeldiambil string  `json:"sampeldiambil"`
	Bildipanggil int 	  `json:"bildipanggil"`
	Gelanghso string      `json:"gelanghso"`
	Annex14 string 	      `json:"annex14"`
	Sampelres string  	  `json:"sampelres"`
	Pelepasan string	  `json:"pelepasan"`	
}

type ACDPeoples struct {
	Peoples []ACDPeople 	`json:"peoples"`
}

type BilKategoriKes struct {
	BilBergejala int 	  `json:"bilBergejala"`
	BilWargaemas int 	  `json:"bilWargaemas"`
}

func GetLawatanRumah(conn *pgxpool.Pool, lr LawatanRumah) ([]byte, error) {
	sql :=
		`select bilrumahk, bilrumahp
		 from acd.house
		 where tarikhacd=$1
		   and locality=$2
		   and district=$3
		   and state=$4`

	row := conn.QueryRow(context.Background(), sql, 
		lr.TarikhACD, lr.Locality, lr.District, lr.State)
	var bilrumahk int 
	var bilrumahp int 	
	err := row.Scan(&bilrumahk, &bilrumahp)
	if err != nil {
		if err == pgx.ErrNoRows { 			
			lrNotFound := LawatanRumah{
                District: "NOTFOUND",
            }
            outputJson, err := json.MarshalIndent(lrNotFound, "", "\t")
			return outputJson, err
		} 
		return nil, err
	}
	rumah := LawatanRumah{				
		Bilrumahk: bilrumahk,
		Bilrumahp: bilrumahp,
	}
	outputJson, err := json.MarshalIndent(rumah, "", "\t")
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

func UpsertLawatanRumah(conn *pgxpool.Pool, lr LawatanRumah) error {
	var err error 

	if lr.Bilrumahp == 0 {	
		sql :=
			`insert into acd.house
			(
				tarikhacd, locality, district, state,
				bilrumahk
			)
			values
			(
				$1, $2, $3, $4, $5
			) 
			on conflict on constraint house_tarikhacd_locality_key
			do 
				update set bilrumahk=house.bilrumahk+1
				where house.tarikhacd=$1
				and house.locality=$2
				and house.district=$3
				and house.state=$4`

		_, err = conn.Exec(context.Background(), sql, 
			lr.TarikhACD, lr.Locality, lr.District, 
			lr.State, lr.Bilrumahk)
	} else if lr.Bilrumahk == 0{ 
		sql :=
			`insert into acd.house
			(
				tarikhacd, locality, district, state,
				bilrumahp
			)
			values
			(
				$1, $2, $3, $4, $5
			) 
			on conflict on constraint house_tarikhacd_locality_key
			do 
				update set bilrumahp=house.bilrumahp+1
				where house.tarikhacd=$1
				and house.locality=$2
				and house.district=$3
				and house.state=$4`

		_, err = conn.Exec(context.Background(), sql, 
			lr.TarikhACD, lr.Locality, lr.District, 
			lr.State, lr.Bilrumahp)
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
			 from acd.acdactivity 
			 where tarikhacd=$1
			   and locality=$2
			   and district=$3
			   and state=$4
			   and kategorikes='Bergejala') as bilbergejala
		 from acd.acdactivity 
		 where tarikhacd=$1
		   and locality=$2
		   and district=$3
		   and state=$4
		   and kategorikes='Warga Emas Perlu Disaring'`

	row := conn.QueryRow(context.Background(), sql, 
		lr.TarikhACD, lr.Locality, lr.District, lr.State)
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

	sql1 :=
		`insert into acd.people
		(
			ident, name, tel, address, locality,
			district, state, comorbid
		)
		 values
		(
			$1, $2, $3, $4, $5, $6, $7, $8
		)`

	_, err := conn.Exec(context.Background(), sql1, 
		ap.Ident, ap.Name, ap.Tel, ap.Address, ap.Locality,
		ap.District, ap.State, ap.Comorbid)
	if err != nil {
		return err
	}

	sql2 :=
		`insert into acd.acdactivity
		(
			tarikhacd, peopleident, locality,
			district, state, kategorikes, gejala
		)
		 values
		(
			$1, $2, $3, $4, $5, $6, $7
		)`

	_, err = conn.Exec(context.Background(), sql2, 
		ap.TarikhACD, ap.Ident, ap.Locality, ap.District, ap.State, 
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
    
func GetSaringan(conn *pgxpool.Pool, lr LawatanRumah) ([]byte, error) {
	// sql :=
	// 	`select ident, name, tel, address, kategorikes,
	// 	 coalesce(jenissampel, '') as jenissampel,
	// 	 coalesce(sampeltca::text, '') as sampeltca, 
	// 	 coalesce(sampeldiambil, '') as sampeldiambil, 
	// 	 coalesce(bildipanggil, 0) as bildipanggil,
	// 	 coalesce(gelanghso, '') as gelanghso, 
	// 	 coalesce(annex14, '') as annex14,
	// 	 coalesce(sampelres, '') as sampelres, 
	// 	 coalesce(pelepasan, '') as pelepasan
	// 	 from acd.people
	// 	 where tarikhacd=$1
	// 	 and locality=$2
	// 	 and district=$3
	// 	 and state=$4`	
	
	sql :=
		`select p.ident, p.name, p.tel, p.address,
		 coalesce(p.comorbid, '') as comorbid,
		 coalesce(p.gelanghso, '') as gelanghso, 
		 coalesce(p.annex14, '') as annex14,
		 coalesce(p.pelepasan, '') as pelepasan,
		 coalesce(a.kategorikes, '') as kategorikes,
		 coalesce(a.gejala, '') as gejala,
		 coalesce(s.jenissampel, '') as jenissampel,
		 coalesce(s.sampeltca::text, '') as sampeltca,
		 coalesce(s.sampeldiambil, '') as sampeldiambil, 
		 coalesce(s.bildipanggil, 0) as bildipanggil,
		 coalesce(s.sampelres, '') as sampelres
		 from acd.acdactivity a
		   join acd.people p
		     on a.peopleident = p.ident	
		   join acd.sampel s
		     on p.ident = s.peopleident	   
		 where a.tarikhacd::text=$1
		 and a.locality=$2
		 and a.district=$3
		 and a.state=$4`

	rows, err := conn.Query(context.Background(), sql, 
		lr.TarikhACD, lr.Locality, lr.District, lr.State)
	if err != nil {
		return nil, err
	}
	var acdPeoples ACDPeoples

	for rows.Next() {
		var ident string 
		var name string 
		var tel string 
		var address string 
		var comorbid string 
		var gelanghso string  
		var annex14 string 
		var pelepasan string 
		var	kategorikes string
		var gejala string 
		var jenissampel string 
		// // var sampeltca time.Time 
		var sampeltca string
		var sampeldiambil string  
		var bildipanggil int 
		var sampelres string 
			
		err := rows.Scan(&ident, &name, &tel, &address, 
			&comorbid, &gelanghso, &annex14, &pelepasan,
			&kategorikes, &gejala, &jenissampel, &sampeltca,
			&sampeldiambil, &bildipanggil, &sampelres)	
		if err != nil {		
			return nil, err
		}
		
		acdPeople := ACDPeople{				
			Ident: ident,
			Name: name,
			Tel: tel,
			Address: address,
			Comorbid: comorbid,	
			Gelanghso: gelanghso,
			Annex14: annex14,
			Pelepasan: pelepasan,
			Kategorikes: kategorikes,
			Gejala: gejala,
			Jenissampel: jenissampel,
			Sampeltca: sampeltca,
			Sampeldiambil: sampeldiambil,
			Bildipanggil: bildipanggil,
			Sampelres: sampelres,
		}
		fmt.Printf("Single struct: %+v\n", acdPeople)
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
	fmt.Printf("%+v\n", lr)
    
    db.CheckDbConn()
    saringanJson, err := GetSaringan(db.Conn, lr)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }   
	fmt.Printf("%s\n", saringanJson)
    fmt.Fprintf(w, "%s", saringanJson)
}
	
func UpdateACDPeople(conn *pgxpool.Pool, ap ACDPeople) error {
	sql := 
		`update acd.people
		   set name=$1, tel=$2, address=$3, locality=$4,
		     district=$5, state=$6, comorbid=$7, gelanghso=$8,
			 annex14=$9, pelepasan=$10
		   where ident=$11`

	_, err := conn.Exec(context.Background(), sql,
		ap.Name, ap.Tel, ap.Address, ap.Locality, ap.District,
		ap.State, ap.Comorbid, ap.Gelanghso, ap.Annex14,
		ap.Pelepasan, ap.Ident)                         
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
	
func UpdateAcdactivity(conn *pgxpool.Pool, aa ACDActivity) error {
	sql := 
		`update acd.acdactivity
		   set locality=$1, district=$2, state=$3, kategorikes=$4,
		     gejala=$5
		   where tarikhacd=$6
		     and peopleident=$7`

	_, err := conn.Exec(context.Background(), sql,
		aa.Locality, aa.District, aa.State, 
		aa.Kategorikes, aa.Gejala, aa.TarikhACD,
		aa.Peopleident)                         
	if err != nil {
		return err
	   }
	return nil
}

func UpdateAcdactivityHandler(w http.ResponseWriter, r *http.Request) {
    util.SetDefaultHeader(w)
    if (r.Method == "OPTIONS") { return }
    fmt.Println("[UpdateAcdactivityHandler] request received")    

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
    err = UpdateAcdactivity(db.Conn, aa)
    if err != nil {        
        util.SendInternalServerErrorStatus(w, err)
        return 
    }   
}
	
func UpdateSampel(conn *pgxpool.Pool, s Sampel) error {
	sql := 
		`update acd.acdactivity
		   set jenissampel=$1, sampeltca=$2, sampeldiambil=$3, 
		   bildipanggil=$4, sampelres=$5
		 where peopleident=$6`

	_, err := conn.Exec(context.Background(), sql,
		s.Jenissampel, s.Sampeltca, s.Sampeldiambil,
		s.Bildipanggil, s.Sampelres, s.Peopleident)                         
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