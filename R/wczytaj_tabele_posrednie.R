#' @title Wczytanie tabel "posrednich" do relacyjnej bazy danych
#' @description Funkcja wczytuje do relacyjnej bazy danych zawartość tabel
#' *pośrednich*, które zostały wcześniej przygotowane jako efekt działania
#' funkcji [przygotuj_tabele_posrednie()].
#' @param tabelePosrednie lista ramek danych zwrócona przez funkcję
#' [przygotuj_tabele_posrednie()] (może zawierać wszystkie 6 tabel
#' *p1*, *p2*, *p3*, *p4*, *p5* i *p6*, lub tylko niektóre z nich)
#' @param baza uchwyt połączenia do bazy lub lista argumentów do funkcji
#' [DBI::dbConnect()] umożliwiających nawiązanie połączenia z bazą
#' danych, w której mają zostać zapisane wczytywane dane
#' @return `NULL`
#' @importFrom dplyr %>% across mutate select where
#' @importFrom DBI dbConnect dbExecute dbDisconnect
#' @export
wczytaj_tabele_posrednie <- function(tabelePosrednie, baza) {
  stopifnot(is.list(tabelePosrednie),
            any(c("p1", "p2", "p3", "p4", "p5", "p6") %in%
                  tolower(names(tabelePosrednie))),
            all(tolower(names(tabelePosrednie)) %in%
                  c("p1", "p2", "p3", "p4", "p5", "p6")),
            all(sapply(tabelePosrednie, is.data.frame)),
            is.list(baza) | inherits(baza, "DBIConnection"))
  if (is.list(baza)) {
    con = do.call(dbConnect, args = baza)
  } else {
    con = baza
  }
  on.exit({if (!inherits(baza, "DBIConnection")) dbDisconnect(con)})
  dbExecute(con, "BEGIN;")
  cat("\nStart: ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), "\n", sep = "")

  if ("p1" %in% names(tabelePosrednie)) {
    cat("\nZapis tabeli P1 do bazy danych...")
    dbExecute(con,
              "INSERT INTO p1 (id_abs, rok_abs, rodzaj_dyplomu, dyplom_szczegoly,
                               rok, miesiac, mies_od_ukoncz, okres,
                               lp_dyplom, kod_zaw, branza,
                               kod_zaw_dyplom, branza_dyplom, dziedzina,
                               dyscyplina_wiodaca)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                         $14, $15)",
              params = tabelePosrednie$p1 %>%
                select("id_abs", "rok_abs", "rodzaj_dyplomu", "dyplom_szczegoly",
                       "rok", "miesiac", "mies_od_ukoncz", "okres",
                       "lp_dyplom", "kod_zaw", "branza",
                       "kod_zaw_dyplom", "branza_dyplom", "dziedzina",
                       "dyscyplina_wiodaca") %>%
                mutate(across(where(is.factor), as.character)) %>%
                as.list() %>%
                unname())
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  if ("p2" %in% names(tabelePosrednie)) {
    cat("\nZapis tabeli P2 do bazy danych...")
    dbExecute(con,
              "INSERT INTO p2 (id_abs, rok_abs, rok, miesiac, mies_od_ukoncz,
                               okres, lp_kont, kod_zaw, branza,
                               id_szk_kont, typ_szk_kont, forma_kont,
                               teryt_pow_kont, kod_zaw_kont, branza_kont,
                               dziedzina_kont, dyscyplina_wiodaca_kont,
                               zrodlo)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                         $14, $15, $16, $17, $18)",
              params = tabelePosrednie$p2 %>%
                select("id_abs", "rok_abs", "rok", "miesiac", "mies_od_ukoncz",
                       "okres", "lp_kont", "kod_zaw", "branza",
                       "id_szk_kont", "typ_szk_kont", "forma_kont",
                       "teryt_pow_kont", "kod_zaw_kont", "branza_kont",
                       "dziedzina_kont", "dyscyplina_wiodaca_kont",
                       "zrodlo") %>%
                mutate(across(where(is.factor), as.character),
                       teryt_pow_kont = 100L * .data$teryt_pow_kont) %>%
                as.list() %>%
                unname())
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  if ("p3" %in% names(tabelePosrednie)) {
    cat("\nZapis tabeli P3 do bazy danych...")
    dbExecute(con,
              "INSERT INTO p3 (id_abs, rok_abs, rok, miesiac, mies_od_ukoncz,
                               okres, status, zmarl, brak_danych_z_zus,
	                             praca, mlodociany, kont_mlodoc_prac, bezrobocie,
	                             bezrobocie_staz, dziecko, macierz, wychow,
	                             pomoc_spol, emeryt_rencista, niepelnosprawny,
                               biernosc_zus, wypadek, choroba, choroba_macierz,
                               wynagrodzenie, wynagrodzenie_uop,
                               teryt_zam, powiat_bezrobocie, powiat_sr_wynagrodzenie,
                               nauka, nauka2, nauka_szk_abs, nauka_bs1st,
                               nauka_bs2st, nauka_technikum, nauka_lo,
                               nauka_spolic, nauka_artystyczna, nauka_sspdp,
                               nauka_kpsp, nauka_studia, nauka_kkz, nauka_kuz)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                         $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24,
                         $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35,
                         $36, $37, $38, $39, $40, $41, $42, $43)",
              params = tabelePosrednie$p3 %>%
                select("id_abs", "rok_abs", "rok", "miesiac", "mies_od_ukoncz",
                       "okres", "status", "zmarl", "brak_danych_z_zus",
                       "praca", 'mlodociany', "kont_mlodoc_prac", "bezrobocie",
                       "bezrobocie_staz", "dziecko", "macierz", "wychow",
                       "pomoc_spol", "emeryt_rencista", "niepelnosprawny",
                       'biernosc_zus', "wypadek", 'choroba', "choroba_macierz",
                       "wynagrodzenie", "wynagrodzenie_uop",
                       "teryt_zam", "powiat_bezrobocie", "powiat_sr_wynagrodzenie",
                       "nauka", "nauka2", "nauka_szk_abs", "nauka_bs1st",
                       "nauka_bs2st", "nauka_technikum", "nauka_lo",
                       "nauka_spolic", "nauka_artystyczna", "nauka_sspdp",
                       "nauka_kpsp", "nauka_studia", "nauka_kkz", "nauka_kuz") %>%
                mutate(across(where(is.factor), as.character),
                       teryt_zam = 100L * .data$teryt_zam) %>%
                as.list() %>%
                unname())
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  if ("p4" %in% names(tabelePosrednie)) {
    cat("\nZapis tabeli P4 do bazy danych...")
    dbExecute(con,
              "INSERT INTO p4 (id_abs, rok_abs, rok_ur, plec, id_szk,
                               duplikat_w_szkole, duplikat_wiele_szkol,
                               mlodoc_byl, typ_szk, szk_specjalna, typ_szk_mlodoc,
                               teryt_pow_szk, nazwa_pow_szk, teryt_woj_szk,
                               nazwa_woj_szk, nazwa_makroreg_szk, nazwa_reg_szk,
                               nazwa_podreg_szk, nts_podreg_szk,
                               lp, kod_zaw, nazwa_zaw, branza, kod_isced,
                               grupa_isced, podgrupa_isced, nazwa_isced,
                               l_prac_ucz_uop, l_prac_nucz_uop, l_prac_nucz_nuop,
                               zawod_sr_wynagrodzenie, abs_w_cke,
                               abs_w_sio, abs_w_polon, abs_w_zus)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                         $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24,
                         $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35)",
              params = tabelePosrednie$p4 %>%
                select("id_abs", "rok_abs", "rok_ur", "plec", "id_szk",
                       "duplikat_w_szkole", "duplikat_wiele_szkol",
                       "mlodoc_byl", "typ_szk", "szk_specjalna", "typ_szk_mlodoc",
                       "teryt_pow_szk", "nazwa_pow_szk",
                       'teryt_woj_szk', "nazwa_woj_szk", "nazwa_makroreg_szk",
                       "nazwa_reg_szk", "nazwa_podreg_szk", "nts_podreg_szk",
                       "lp", "kod_zaw", 'nazwa_zaw', "branza",
                       "kod_isced", "grupa_isced", "podgrupa_isced", "nazwa_isced",
                       "l_prac_ucz_uop", "l_prac_nucz_uop", "l_prac_nucz_nuop",
                       "zawod_sr_wynagrodzenie", "abs_w_cke", "abs_w_sio",
                       "abs_w_polon", "abs_w_zus") %>%
                mutate(across(where(is.factor), as.character),
                       teryt_pow_szk = 100L * .data$teryt_pow_szk,
                       teryt_woj_szk = 10000L * .data$teryt_woj_szk) %>%
                as.list() %>%
                unname())
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  if ("p5" %in% names(tabelePosrednie)) {
    cat("\nZapis tabeli P5 do bazy danych...")
    dbExecute(con,
              "INSERT INTO p5 (id_abs, rok_abs, rok, miesiac, mies_od_ukoncz,
                               okres, lp_pracod, pkd_pracod, forma_zatrudnienia,
                               mlodociany, wynagrodzenie, wynagrodzenie_uop)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
              params = tabelePosrednie$p5 %>%
                select("id_abs", "rok_abs", "rok", "miesiac", "mies_od_ukoncz",
                       "okres", 'lp_pracod', "pkd_pracod", "forma_zatrudnienia",
                       "mlodociany", "wynagrodzenie", "wynagrodzenie_uop") %>%
                mutate(across(where(is.factor), as.character)) %>%
                as.list() %>%
                unname())
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  if ("p6" %in% names(tabelePosrednie)) {
    cat("\nZapis tabeli P6 do bazy danych...")
    dbExecute(con,
              "INSERT INTO p6 (id_szk, szk_ma_abs,
                               typ_szk, typ_szk_sdd, typ_szk_rspo,
                               publicznosc, kategoria_uczniow, specyfika,
	                             organ_rejestrujacy_id, organ_rejestrujacy_typ,
	                             organ_rejestrujacy_nazwa,
	                             organ_rejestrujacy_teryt,
	                             organ_sposob_ewidencjonowania,
	                             rok_szk, nazwa_szk, teryt_gmi_szk,
	                             wojewodztwo_szk, powiat_szk, gmina_szk,
	                             simc_miejsc, miejscowosc, rodzaj_miejsc,
	                             sym_ul, ulica, nr_budynku, nr_lokalu, pna, poczta,
                               organ_prowadzacy_typ, organ_prowadzacy_nazwa,
                               organ_prowadzacy_regon, organ_prowadzacy_teryt,
                               organ_prowadzacy_woj, organ_prowadzacy_pow,
                               organ_prowadzacy_gmi, miejsce_w_strukt,
                               jedn_nadrz_id, jedn_nadrz_typ)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                         $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24,
                         $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35,
                         $36, $37, $38)",
              params = tabelePosrednie$p6 %>%
                select("id_szk", "szk_ma_abs",
                       "typ_szk", "typ_szk_sdd", "typ_szk_rspo",
                       "publicznosc", "kategoria_uczniow", "specyfika",
                       "organ_rejestrujacy_id", "organ_rejestrujacy_typ",
                       "organ_rejestrujacy_nazwa", "organ_rejestrujacy_teryt",
                       "organ_sposob_ewidencjonowania",
                       "rok_szk", "nazwa_szk", "teryt_gmi_szk",
                       "wojewodztwo_szk", "powiat_szk", "gmina_szk",
                       "simc_miejsc", "miejscowosc", "rodzaj_miejsc", "sym_ul",
                       "ulica", "nr_budynku", "nr_lokalu", "pna", "poczta",
                       "organ_prowadzacy_typ", "organ_prowadzacy_nazwa",
                       "organ_prowadzacy_regon", "organ_prowadzacy_teryt",
                       "organ_prowadzacy_woj", "organ_prowadzacy_pow",
                       "organ_prowadzacy_gmi", "miejsce_w_strukt",
                       "jedn_nadrz_id", "jedn_nadrz_typ") %>%
                mutate(across(where(is.factor), as.character),
                       organ_prowadzacy_teryt =
                         100L * .data$organ_prowadzacy_teryt) %>%
                as.list() %>%
                unname())
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  cat("\n")
  dbExecute(con, "COMMIT;")
  invisible(NULL)
}
