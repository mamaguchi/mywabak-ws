package main

import (
	"net/http"
	"mywabak/webservice/db"
	"mywabak/webservice/data/mywabak"
	"mywabak/webservice/auth"
)

func main() {
	/* INIT DATABASE CONNECTION */
	// defer data.Close()	
	db.Open()
	defer db.Close()

	/* HANDLER FUNC */
	// myWabak
	http.HandleFunc("/poscases", mywabak.GetPosCasesPCRHandler)
	http.HandleFunc("/closecontacts", mywabak.GetCloseContactsHandler)
	http.HandleFunc("/people/get", mywabak.GetPeopleBasicHandler)
	http.HandleFunc("/people/add", mywabak.AddNewPeopleHandler)
	http.HandleFunc("/people/update", mywabak.UpdatePeopleHandler)
	http.HandleFunc("/wbkcase/people/add", mywabak.AddPeopleToWbkcaseHandler)
	http.HandleFunc("/wbkcase/people/update", mywabak.UpdatePeopleInWbkcaseHandler)
	http.HandleFunc("/wbkcase/people/del", mywabak.DelPeopleFromWbkcaseHandler)
	http.HandleFunc("/hso/add", mywabak.AddNewHSOHandler)
	http.HandleFunc("/hso/update", mywabak.UpdateHSOHandler)
	http.HandleFunc("/hso/del", mywabak.DelHSOHandler)
	// ACD
	http.HandleFunc("/acd/rumah/get", mywabak.GetLawatanRumahHandler)
	http.HandleFunc("/acd/rumah/upsert", mywabak.UpsertLawatanRumahHandler)
	http.HandleFunc("/acd/saringan/get/kategorikes", mywabak.GetKategoriKesSaringanHandler)
	http.HandleFunc("/acd/saringan/add", mywabak.AddSaringanHandler)
	http.HandleFunc("/acd/saringan/peoples/get", mywabak.GetSaringanHandler)
	http.HandleFunc("/acd/saringan/people/update", mywabak.UpdateACDPeopleHandler)
	http.HandleFunc("/acd/saringan/acdactivity/update", mywabak.UpdateAcdactivityHandler)
	http.HandleFunc("/acd/saringan/sampel/update", mywabak.UpdateSampelHandler)
	// Auth
	http.HandleFunc("/signup", auth.SignUpPeopleHandler)
	http.HandleFunc("/signin", auth.BindHandler)
	// People
	// http.HandleFunc("/people/search", data.SearchPeopleHandler)
	// http.HandleFunc("/people/create", data.CreateNewPeopleHandler)
	// http.HandleFunc("/people/get", data.GetPeopleHandler)
	// http.HandleFunc("/people/update", data.UpdatePeopleHandler)
	// http.HandleFunc("/people/delete", data.DeletePeopleHandler)
	// http.HandleFunc("/vacrec/create", data.CreateNewVacRecHandler)
	// http.HandleFunc("/vacrec/get", data.GetCovidVacRecHandler)
	// http.HandleFunc("/vacrec/update", data.UpdateVacRecHandler)
	// http.HandleFunc("/vacrec/delete", data.DeleteVacRecHandler)

	/* START HTTP SERVER */
	http.ListenAndServe(":8080", nil)
}