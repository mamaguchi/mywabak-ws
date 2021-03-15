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
	http.HandleFunc("/cc/get", mywabak.GetCloseContactsHandler)
	http.HandleFunc("/people/get", mywabak.GetPeopleBasicHandler)
	http.HandleFunc("/people/upsert", mywabak.UpsertPeopleHandler)
	http.HandleFunc("/people/update", mywabak.UpdatePeopleHandler)
	http.HandleFunc("/wbkcase/people/add", mywabak.AddPeopleToWbkcaseHandler)
	http.HandleFunc("/wbkcase/people/reg", mywabak.RegNewCloseContactHandler)
	http.HandleFunc("/wbkcase/people/update", mywabak.UpdatePeopleInWbkcaseHandler)
	http.HandleFunc("/wbkcase/people/del", mywabak.DelPeopleFromWbkcaseHandler)
	http.HandleFunc("/hso/add", mywabak.AddNewHSOHandler)
	http.HandleFunc("/hso/update", mywabak.UpdateHSOHandler)
	http.HandleFunc("/hso/del", mywabak.DelHSOHandler)
	// ACD
	http.HandleFunc("/acd/get", mywabak.GetACDListHandler)
	http.HandleFunc("/acd/upsert", mywabak.UpsertACDHandler)
	http.HandleFunc("/acd/districts/get", mywabak.GetDistrictsHandler)
	http.HandleFunc("/acd/localities/get", mywabak.GetLocalitiesHandler)
	http.HandleFunc("/acd/rumah/get", mywabak.GetLawatanRumahHandler)
	http.HandleFunc("/acd/rumah/upsert", mywabak.UpsertLawatanRumahHandler)
	http.HandleFunc("/acd/saringan/get/kategorikes", mywabak.GetKategoriKesSaringanHandler)
	http.HandleFunc("/acd/saringan/add", mywabak.AddSaringanHandler)
	http.HandleFunc("/acd/saringan/people/get", mywabak.GetSaringanBasicHandler)
	http.HandleFunc("/acd/saringan/peoples/get", mywabak.GetSaringanHandler)
	http.HandleFunc("/acd/saringan/hsoandsampel/get", mywabak.GetHSOandSampleHandler)
	http.HandleFunc("/acd/saringan/people/update", mywabak.UpdateACDPeopleHandler)
	http.HandleFunc("/acd/saringan/people/updateoc", mywabak.UpdateACDPeopleOneColHandler)
	http.HandleFunc("/acd/saringan/hso/update", mywabak.UpdateACDHsoHandler)
	http.HandleFunc("/acd/saringan/hso/updateoc", mywabak.UpdateACDHsoOneColHandler)
	http.HandleFunc("/acd/saringan/acdactivity/update", mywabak.UpdateACDActivityHandler)
	http.HandleFunc("/acd/saringan/acdactivity/updateoc", mywabak.UpdateACDactivityOneColHandler)
	http.HandleFunc("/acd/saringan/sampel/update", mywabak.UpdateSampelHandler)
	http.HandleFunc("/acd/saringan/sampel/updateoc", mywabak.UpdateSampelOneColHandler)
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