#' @title Przygotowanie danych do tabeli wejsciowej W26
#' @description Funkcja przekształca dane o szkołach wyeksportowane
#' z (publicznego API WWW) RSPO i, opcjonalnie, dane o *idPodmiotu* SIO organów
#' prowadzących szkół, wyeksportowane z hurtowni danych SIO do struktury, jaka
#' jest oczekiwana w tabeli *wejściowej* W26. Sprawdza przy tym, czy w danych
#' z RSPO znajdują się wszystkie szkoły, które występują w danych z danej edycji
#' monitoringu (plikach *W2.csv*, *W3.csv*, *W4.csv* i *W5.csv*)
#' @param rokMonitoringu rok (edycja) prowadzenie monitoringu (liczba)
#' @param rspo opcjonalnie wektor ciągów znaków z nazwami plików CSV
#' zawierającymi dane wyeksportowane z (publicznego API WWW) RSPO (lub pozyskane
#' w inny sposób, ale zapisane w tej samej strukturze)
#' @param idOrgProw opcjonalnie nazwa pliku zawierającego dane o *idPodmiotu*
#' SIO organów prowadzących szkół, wyeksportowane z hurtowni danych SIO; można
#' też podać `NULL`, aby nie dołączać tych danych (w szczególności, gdy nie są
#' dostępne)
#' @param zapiszDoPliku opcjonalnie ciąg znaków z nazwą pliku CSV, do którego ma
#' zostać zapisane przygotowane zestawienie; aby uniknąć zapisu do pliku, jako
#' wartość argumentu należy podać `NULL`
#' @returns ramka danych w formacie odpowiadającym specyfikacji
#' tabeli *wejściowej* W26; jeśli nastąpił zapis do pliku CSV, wynik zwracany
#' jest jako *ukryty*
#' @importFrom stats setNames
#' @importFrom utils read.csv2 write.csv2
#' @importFrom dplyr %>% .data arrange bind_rows distinct everything inner_join
#'                  left_join mutate select rename_with
#' @export
przygotuj_dane_do_w26 <- function(rokMonitoringu,
                                  rspo = list.files(pattern = "^rspo_.*\\.csv$"),
                                  idOrgProw = "id-organow-prowadzacych.csv",
                                  zapiszDoPliku = "W26.csv") {
  stopifnot(is.numeric(rokMonitoringu), length(rokMonitoringu) == 1L,
            !is.na(rokMonitoringu), rokMonitoringu >= 2021L,
            as.integer(rokMonitoringu) == rokMonitoringu,
            is.character(rspo), length(rspo) > 0L, !anyNA(rspo), all(rspo != ""),
            all(file.exists(rspo)), all(file.access(rspo, mode = 4) == 0L),
            is.null(idOrgProw) | is.character(idOrgProw),
            is.null(zapiszDoPliku) | is.character(zapiszDoPliku),
            "W katalogu roboczym nie ma pliku 'W2.csv'." =
              file.exists("W2.csv"),
            "Nie można uzyskać dostępu do pliku 'W2.csv'" =
              file.access("W2.csv", mode = 4) == 0L,
            "W katalogu roboczym nie ma pliku 'W3.csv'." =
              file.exists("W3.csv"),
            "Nie można uzyskać dostępu do pliku 'W3.csv'" =
              file.access("W3.csv", mode = 4) == 0L,
            "W katalogu roboczym nie ma pliku 'W4.csv'." =
              file.exists("W4.csv"),
            "Nie można uzyskać dostępu do pliku 'W4.csv'" =
              file.access("W4.csv", mode = 4) == 0L,
            "W katalogu roboczym nie ma pliku 'W5.csv'." =
              file.exists("W5.csv"),
            "Nie można uzyskać dostępu do pliku 'W5.csv'" =
              file.access("W5.csv", mode = 4) == 0L)
  if (!is.null(idOrgProw)) {
    stopifnot(length(idOrgProw) == 1L, !is.na(idOrgProw), idOrgProw != "",
              file.exists(idOrgProw), file.access(idOrgProw, mode = 4) == 0L)
  }
  if (!is.null(zapiszDoPliku)) {
    stopifnot(length(zapiszDoPliku) == 1L, !anyNA(zapiszDoPliku))
  }
  kolumnyRSPO <- c(ID_SZK = "NUMER.RSPO", TYP_SZK = "TYP", NAZWA_SZK = "NAZWA",
                   TERYT_GMI_SZK = "KOD.TERYTORIALNY.GMINA",
                   WOJEWODZTWO_SZK = "WOJEWÓDZTWO",
                   POWIAT_SZK = "POWIAT", GMINA_SZK = "GMINA",
                   SIMC_MIEJSC = "KOD.TERYTORIALNY.MIEJSCOWOŚĆ",
                   MIEJSCOWOSC = "MIEJSCOWOŚĆ",
                   RODZAJ_MIEJSC = "RODZAJ.MIEJSCOWOŚCI",
                   SYM_UL = "KOD.TERYTORIALNY.ULICA",
                   ULICA = "ULICA", NR_BUDYNKU = "NUMER.BUDYNKU",
                   NR_LOKALU = "NUMER.LOKALU",
                   PNA = "KOD.POCZTOWY", POCZTA = "POCZTA",
                   PUBLICZNOSC = "PUBLICZNOŚĆ.STATUS",
                   KATEGORIA_UCZNIOW = "KATEGORIA.UCZNIÓW",
                   SPECYFIKA = "SPECYFIKA.PLACÓWKI",
                   ORGAN_PROWADZACY_TYP = "TYP.ORGANU.PROWADZĄCEGO",
                   ORGAN_PROWADZACY_NAZWA = "NAZWA.ORGANU.PROWADZĄCEGO",
                   ORGAN_PROWADZACY_REGON = "REGON.ORGANU.PROWADZĄCEGO",
                   ORGAN_PROWADZACY_WOJ = "WOJEWÓDZTWO.ORGANU.PROWADZĄCEGO",
                   ORGAN_PROWADZACY_POW = "POWIAT.ORGANU.PROWADZĄCEGO",
                   ORGAN_PROWADZACY_GMI = "GMINA.ORGANU.PROWADZĄCEGO",
                   MIEJSCE_W_STRUKT = "MIEJSCE.W.STRUKTURZE",
                   JEDN_NADRZ_ID = "RSPO.PODMIOTU.NADRZĘDNEGO",
                   JEDN_NADRZ_TYP = "TYP.PODMIOTU.NADRZĘDNEGO")

  rspo <- setNames(lapply(rspo, read.csv2), rspo)
  czyProblem <- FALSE
  message("Rok monitoringu: ", rokMonitoringu)
  message("Wczytywanie plików z danymi z RSPO:")
  for (p in seq_along(rspo)) {
    names(rspo[[p]]) <- toupper(names(rspo[[p]]))
    brakujaceKolumny <- setdiff(kolumnyRSPO, names(rspo[[p]]))
    if (length(brakujaceKolumny) > 0L) {
      czyProblem <- czyProblem | TRUE
    }
    message("- plik '", names(rspo)[p], "' ",
            ifelse(length(brakujaceKolumny) == 0L,
                   "ma wszystkie wymagane kolumny;",
                   paste0("brakuje kolumn (z dokładnością do wielkości liter): '",
                          paste(brakujaceKolumny, collapse = "', '"), "';")))
  }
  message("...zakończone.")
  stopifnot("We wczytywanym pliku/plikach brakuje wymaganych kolumn." = !czyProblem)
  rspo <- bind_rows(rspo)
  rspo <- rspo[, kolumnyRSPO]
  names(rspo) <- names(kolumnyRSPO)

  message("Wczytywanie plików 'W2.csv', 'W3.csv', 'W4.csv' i 'W5.csv'...")
  w2 <- read.csv2("W2.csv")
  stopifnot(all(c("ID_SZK", "ROK_ABS") %in% names(w2)))
  w3 <- read.csv2("W3.csv")
  stopifnot(all(c("ID_SZK_KONT", "DATA_OD_SZK_KONT",
                  "DATA_DO_SZK_KONT") %in% names(w3)))
  w4 <- read.csv2("W4.csv")
  stopifnot(all(c("ID_SZK_KONT", "DATA_OD_KKZ", "DATA_DO_KKZ") %in% names(w4)))
  w5 <- read.csv2("W5.csv")
  stopifnot(all(c("ID_SZK_KONT", "DATA_OD_KUZ", "DATA_DO_KUZ") %in% names(w5)))
  message("...zakończone.")

  if (!is.null(idOrgProw)) {
    message("Wczytywanie danych o idPodmiotu SIO organów prowadzących...")
    stopifnot(length(idOrgProw) == 1L, !is.na(idOrgProw), idOrgProw != "")
    idOrgProwDane <- read.csv2(idOrgProw)
    names(idOrgProwDane) <- toupper(names(idOrgProwDane))
    kolumnyIdOrgProw <- c("ID_SZK", "ORGAN_PROWADZACY_SPOSOB",
                          "ORGAN_PROWADZACY_ID")
    brakujaceKolumny <- setdiff(kolumnyIdOrgProw, names(idOrgProwDane))
    if (length(brakujaceKolumny) > 0L) {
      stop("W pliku '", idOrgProw, "' brakuje wymaganych kolumn: '",
           paste(brakujaceKolumny, collapse = "', '"), "'.")
    }
    rspo <- left_join(rspo, idOrgProwDane[, kolumnyIdOrgProw], by = "ID_SZK")
    message("...zakończone.")
  }

  message("Sprawdzanie kompletności danych...")
  rspo <- rspo %>%
    left_join(rspo %>%
                select(ORGAN_PROWADZACY_TERYT = "TERYT_GMI_SZK",
                       "WOJEWODZTWO_SZK", "POWIAT_SZK") %>%
                mutate(ORGAN_PROWADZACY_TERYT =
                         100 * floor(.data$ORGAN_PROWADZACY_TERYT / 1000)) %>%
                distinct() %>%
                rename_with(~paste0("ORGAN_PROWADZACY_", substr(., 1, 3)),
                            .cols = -"ORGAN_PROWADZACY_TERYT"),
              by = c("ORGAN_PROWADZACY_WOJ", "ORGAN_PROWADZACY_POW"))
  szkolyWDanych <- zidentyfikuj_szkolo_lata_w_danych(w2, w3, w4, w5,
                                                     rokMonitoringu)
  message("...zakończone.")
  brakujaceSzkoly <- setdiff(szkolyWDanych$ID_SZK, rspo$ID_SZK)
  if (length(brakujaceSzkoly) > 0L) {
    stop("W danych z RSPO brakuje szkoł o następujących id: ",
         paste(brakujaceSzkoly, collapse = ", "), ".")
  }
  rspo <- rspo %>%
    inner_join(szkolyWDanych,
               by = "ID_SZK") %>%
    select("ID_SZK", "ROK_SZK", everything()) %>%
    arrange(.data$ID_SZK, .data$ROK_SZK)

  if (!is.null(zapiszDoPliku)) {
    zapiszDoPliku <- paste0(sub("\\.csv$", "", zapiszDoPliku), ".csv")
    write.csv2(rspo, zapiszDoPliku, row.names = FALSE, na = "")
    invisible(rspo)
  } else {
    return(rspo)
  }
}
