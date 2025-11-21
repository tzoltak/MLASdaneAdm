#' @title Uzupelnianie wartosci wskaznikow wzglednych wynagrodzen
#' @description Funkcja pozwala uzupełnić wartości wskaźników względnych
#' wynagrodzen (`sr_wynagr_uop_nauka_r0_wrzgru`
#' i `sr_wynagr_uop_bez_nauki_r0_wrzgru`) w tabeli *pośredniej* P4, jeśli
#' wcześniej przygotowane tabele *pośrednie* miały w nich braki danych dlatego,
#' że w momencie ich przygotowywania GUS nie zdążył jeszcze opublikować
#' wskaźnika średnich miesięcznych wynagrodzeń w powiatach dla ostatniego roku,
#' a teraz są już one dostępne (i została przygotowana zaktualizowana,
#' obejmująca je, wersja pliku *wejściowego* W19.csv - p.
#' [przygotuj_dane_do_w19()])
#' @param p4 ramka danych z tabelą *pośrednią* P4, musi zawierać co najmniej
#'  kolumny `id_abs` i `rok_abs`);
#' jeśli w wyniku działania funkcji chce się uzyskać wyłącznie tabelę
#' *pośrednią* P3 z uzupełnionymi wartościami kolumny `powiat_sr_wynagrodzenie`
#' oraz `powiat_bezrobocie`, można ustawić wartość tego argumentu na `NULL`
#' @param p3 ramka danych z tabelą *pośrednią* P3, musi zawierać co najmniej
#'  kolumny `id_abs`, `rok_abs`, `okres`, `wynagrodzenie_uop`, `teryt_zam`
#' i `nauka2`
#' @param plikW19 opcjonalnie ciąg znaków z nazwą pliku CSV z tabelą *wejściową*
#' W19 (p. [przygotuj_dane_do_w19()])
#' @param nieNadpisuj opcjonalnie wartość logiczna - czy uzupełnić wyłącznie te
#' wartości kolumny `powiat_sr_wynagrodzenie` w ramce danych przekazanej
#' argumentem `p3`, które są brakami danych (pozostawiając jak są te, które
#' mają już konkretne wartości)?
#' @returns ramka danych przekazana argumentem `p4`, w której na nowo
#' przeliczone zostały wartości kolumn `sr_wynagr_uop_nauka_r0_wrzgru`
#' i `sr_wynagr_uop_bez_nauki_r0_wrzgru` (lub, jeśli tych kolumn w niej nie
#' było, to zostały do niej dodane) albo ramka danych przekazana argumentem
#' `p3`, w której uzupełniono wartości kolumny `powiat_sr_wynagrodzenie` oraz
#' `powiat_bezrobocie` (jeśli funkcję wywołano z argumentem `p4` ustawionym na
#' `NULL`)
#' @seealso [LOSYwskazniki::dodaj_wskazniki_prace()]
#' @examples
#' \dontrun{
#' library(MLASZdane)
#' # w katalogu roboczym musi znajdować się plik "W19.csv"!
#' p4 <- uzupelnij_wzgledne_wynagrodzenia(p4, p3)
#' p3 <- uzupelnij_wzgledne_wynagrodzenia(NULL, p3)
#' }
#' @importFrom utils read.table
#' @importFrom dplyr %>% .data arrange filter group_by mutate select summarise
#' @importFrom LOSYwskazniki oblicz_wynagrodzenia_wzgledne
#' @export
uzupelnij_wzgledne_wynagrodzenia <- function(p4, p3, plikW19 = "W19.csv",
                                             nieNadpisuj = TRUE) {
  stopifnot(is.data.frame(p3),
            all(c("id_abs", "rok_abs", "okres", "wynagrodzenie_uop",
                  "teryt_zam", "nauka2") %in% names(p3)),
            !("powiat_sr_wynagrodzenie_uzup_" %in% names(p3)),
            !("powiat_bezrobocie_uzup_" %in% names(p3)),
            !anyNA(p3$id_abs), !anyNA(p3$rok_abs), !anyNA(p3$okres),
            is.character(plikW19), length(plikW19) == 1L, !anyNA(plikW19),
            file.exists(plikW19),
            is.logical(nieNadpisuj), length(nieNadpisuj) == 1L,
            !anyNA(nieNadpisuj),
            is.null(p4) | is.data.frame(p4))
  if (!is.null(p4)) {
    stopifnot(is.data.frame(p4),
              all(c("id_abs", "rok_abs") %in% names(p4)),
              !anyNA(p4$id_abs), !anyNA(p4$rok_abs),
              mode(p3$id_abs) == mode(p4$id_abs),
              mode(p3$rok_abs) == mode(p4$rok_abs))
  }
  w19 <- read.table("W19.csv", sep = ";", dec = ".", header = TRUE)
  stopifnot(all(c("ROK", "MIESIAC", "TERYT", "SR_WYNAGRODZENIA") %in%
                  names(w19)))
  w19 <- w19 %>%
    mutate(okres  = 12*.data$ROK + .data$MIESIAC,
           teryt_zam = .data$TERYT %/% 100L) %>%
    select("okres", "teryt_zam",
           powiat_sr_wynagrodzenie_uzup_ = "SR_WYNAGRODZENIA",
           powiat_bezrobocie_uzup_ = "STOPA_BEZROBOCIA") %>%
    filter(!is.na(powiat_sr_wynagrodzenie_uzup_))

  miesOdUkoncz <-  3L:6L
  if (!is.null(p4)) {
    p3 <- p3 %>%
      filter(mies_od_ukoncz %in% miesOdUkoncz)
  }

  p3 <- p3 %>%
    left_join(w19,
              by = c("okres", "teryt_zam"))
  podsumowanie <- p3 %>%
    filter(!is.na(.data$teryt_zam)) %>%
    group_by(.data$rok) %>%
    summarise(nBezSrWynagr = sum(is.na(.data$powiat_sr_wynagrodzenie)),
              nBezSrWynagrUzup = sum(is.na(.data$powiat_sr_wynagrodzenie_uzup_)),
              opis = ifelse(nBezSrWynagr == 0,
                            paste0("- ", .data$rok[1], " rok: żadnych brakujących wartości średnich wynagrodzeń w powiecie;"),
                            paste0("- ", .data$rok[1], " rok:\n  - ",
                                   format(.data$nBezSrWynagr, big.mark = " "),
                                   " rekordów (",
                                   format(round(100 * .data$nBezSrWynagr / n(), 1),
                                          nsmall = 1),
                                   "%) z brakującą wartością średnich wynagrodzeń w powiecie,\n  ",
                                   "a po uzupełnieniu będzie ich:\n  - ",
                                   format(.data$nBezSrWynagrUzup, big.mark = " "),
                                   " (",
                                   format(round(100 * .data$nBezSrWynagr / n(), 1),
                                          nsmall = 1), "%);")),
              .groups = "drop") %>%
    arrange(.data$rok)
  message("Podsumowanie kompletności danych przekazanych argumentem `p3` w poszczególnych latach (biorąc pod uwagę tylko rekordy, dla których `teryt_zam` nie jest brakiem danych",
          ifelse(is.null(p4), "", ", obejmujące okres wykorzystywany do obliczania wskaźników względnego wynagrodzenia"),
          "):\n\n", paste(podsumowanie$opis, collapse = "\n"))

  if (nieNadpisuj) {
    p3$powiat_sr_wynagrodzenie <- ifelse(is.na(p3$powiat_sr_wynagrodzenie),
                                         p3$powiat_sr_wynagrodzenie_uzup_,
                                         p3$powiat_sr_wynagrodzenie)
    p3$powiat_bezrobocie <- ifelse(is.na(p3$powiat_bezrobocie),
                                   p3$powiat_bezrobocie_uzup_,
                                   p3$powiat_bezrobocie)
  } else {
    p3$powiat_sr_wynagrodzenie <- p3$powiat_sr_wynagrodzenie_uzup_
    p3$powiat_bezrobocie <- p3$powiat_bezrobocie_uzup_
  }
  p3 <- p3 %>% select(-c("powiat_sr_wynagrodzenie_uzup_",
                         "powiat_bezrobocie_uzup_"))
  if (is.null(p4)) return(p3)

  nazwyZm <- names(p4)
  pozycje <- which(nazwyZm %in% c("sr_wynagr_uop_nauka_r0_wrzgru",
                                  "sr_wynagr_uop_bez_nauki_r0_wrzgru"))
  p4 <- p4 %>%
    select(-any_of(c("sr_wynagr_uop_nauka_r0_wrzgru",
                     "sr_wynagr_uop_bez_nauki_r0_wrzgru"))) %>%
    left_join(oblicz_wynagrodzenia_wzgledne(p3, miesOdUkoncz = miesOdUkoncz,
                                            nauka2 = 1,
                                            nazwaWsk = "sr_wynagr_uop_nauka_r0_wrzgru"),
              by = c("id_abs", "rok_abs")) %>%
    left_join(oblicz_wynagrodzenia_wzgledne(p3, miesOdUkoncz = miesOdUkoncz,
                                            nauka2 = 0,
                                            nazwaWsk = "sr_wynagr_uop_bez_nauki_r0_wrzgru"),
              by = c("id_abs", "rok_abs"))
  zmPrzed <- names(p4)[names(p4) %in% nazwyZm[setdiff(seq_len(pozycje[1]),
                                                      pozycje[1])]]
  zmPo <- names(p4)[names(p4) %in% nazwyZm[setdiff(seq(pozycje[1],
                                                       length(nazwyZm)),
                                                   pozycje)]]
  return(p4[, c(zmPrzed, "sr_wynagr_uop_nauka_r0_wrzgru",
                "sr_wynagr_uop_bez_nauki_r0_wrzgru", zmPo)])
}
