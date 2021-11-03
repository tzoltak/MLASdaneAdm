#' @title Wczytanie tabel "posrednich" do relacyjnej bazy danych
#' @description Funkcja wczytuje do relacyjnej bazy danych zawartość tabel
#' \emph{pośrednich}, które zostały wcześniej przygotowane jako efekt działania
#' funkcji \code{\link{przygotuj_tabele_posrednie}}.
#' @param tabelePosrednie lista ramek danych zwrócona przez funkcję
#' \code{\link{przygotuj_tabele_posrednie}} (może zawierać wszystkie 4 tabele
#' \emph{p1}, \emph{p2}, \emph{p3} i \emph{p4}, lub tylko niektóre z nich)
#' @param baza uchwyt połączenia do bazy lub lista argumentów do funkcji
#' \code{\link[DBI]{dbConnect}} umożliwiających nawiązanie połączenia z bazą
#' danych, w której mają zostać zapisane wczytwane dane
#' @return NULL
#' @importFrom dplyr %>% select
#' @importFrom DBI dbConnect dbExecute dbDisconnect
#' @export
wczytaj_tabele_posrednie <- function(tabelePosrednie, baza) {
  stopifnot(is.list(tabelePosrednie),
            any(c("p1", "p2", "p3", "p4") %in% tolower(names(tabelePosrednie))),
            all(tolower(names(tabelePosrednie)) %in% c("p1", "p2", "p3", "p4")),
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
                                 okres_dyplom, lp_dyplom, kod_zaw, branza,
                                 kod_zaw_dyplom, branza_dyplom, dziedzina,
                                 dyscyplina_wiodaca)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
              params = tabelePosrednie$p1 %>%
                select(id_abs, rok_abs, rodzaj_dyplomu, dyplom_szczegoly,
                       okres_dyplom, lp_dyplom, kod_zaw, branza, kod_zaw_dyplom,
                       branza_dyplom, dziedzina, dyscyplina_wiodaca) %>%
                as.list() %>%
                unname())
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  if ("p2" %in% names(tabelePosrednie)) {
    cat("\nZapis tabeli P2 do bazy danych...")
    dbExecute(con,
              "INSERT INTO p2 (id_abs, rok_abs, okres_kont, lp_kont, kod_zaw,
                           branza, branza_kont, dziedzina_kont,
                           dyscyplina_wiodaca_kont, branza_kont_zrodlo)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
              params = tabelePosrednie$p2 %>%
                select(id_abs, rok_abs, okres_kont, lp_kont, kod_zaw, branza,
                       branza_kont, dziedzina_kont, dyscyplina_wiodaca_kont,
                       branza_kont_zrodlo) %>%
                as.list() %>%
                unname())
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  if ("p3" %in% names(tabelePosrednie)) {
    cat("\nZapis tabeli P3 do bazy danych...")
    dbExecute(con,
              "INSERT INTO p3 (id_abs, rok_abs, okres, zmarl, status_nieustalony,
	                         praca, mlodociany, bezrobocie, bezrobocie_staz,
	                         dziecko, biernosc, kont_mlodoc_prac, wynagrodzenie,
	                         wynagrodzenie_uop, teryt_zam, powiat_bezrobocie,
	                         powiat_sr_wynagrodzenie, nauka, nauka2, nauka_szk_abs,
                           nauka_bs2st, nauka_lodd, nauka_spolic, nauka_studia,
                           nauka_kkz, nauka_kuz)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                         $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24,
                         $25, $26)",
              params = tabelePosrednie$p3 %>%
                select(id_abs, rok_abs, okres, zmarl, status_nieustalony, praca,
                       mlodociany, bezrobocie, bezrobocie_staz, dziecko, biernosc,
                       kont_mlodoc_prac, wynagrodzenie, wynagrodzenie_uop,
                       teryt_zam, powiat_bezrobocie, powiat_sr_wynagrodzenie,
                       nauka, nauka2, nauka_szk_abs, nauka_bs2st, nauka_lodd,
                       nauka_spolic, nauka_studia, nauka_kkz, nauka_kuz) %>%
                as.list() %>%
                unname())
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  if ("p4" %in% names(tabelePosrednie)) {
    cat("\nZapis tabeli P4 do bazy danych...")
    dbExecute(con,
              "INSERT INTO p4 (id_abs, rok_abs, rok_ur, plec, id_szk, typ_szk,
                           teryt_pow_szk, teryt_woj_szk, lp, kod_zaw, nazwa_zaw,
                           branza, l_prac_ucz_uop, l_prac_nucz_uop,
                           l_prac_nucz_nuop, zawod_sr_wynagrodzenie, abs_w_cke,
                           abs_w_sio, abs_w_polon, abs_w_zus)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                         $14, $15, $16, $17, $18, $19, $20)",
              params = tabelePosrednie$p4 %>%
                select(id_abs, rok_abs, rok_ur, plec, id_szk, typ_szk,
                       teryt_pow_szk, teryt_woj_szk, lp, kod_zaw, nazwa_zaw,
                       branza, l_prac_ucz_uop, l_prac_nucz_uop, l_prac_nucz_nuop,
                       zawod_sr_wynagrodzenie, abs_w_cke, abs_w_sio, abs_w_polon,
                       abs_w_zus) %>%
                as.list() %>%
                unname())
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  cat("\n")
  dbExecute(con, "COMMIT;")
  invisible(NULL)
}
