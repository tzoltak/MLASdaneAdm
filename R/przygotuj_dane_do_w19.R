#' @title Przygotowanie danych do tabeli wejsciowej W19
#' @description Funkcja przekształca zestawienia wskaźników bezrobocia
#' i przeciętnych (w skali roku) miesięcznych wynagrodzeń, przekazane do niej
#' w formie, w jakiej są zwracane przez [MLASZdane::pobierz_dane_bdl] do formatu
#' wymaganego dla tabeli *wejściowej* W19. O ile podano argument
#' `wynagrodzeniaWoj`, próbuje też zaimputować ew. braki danych we wskaźniku
#' przeciętnych wynagrodzeń występujące na poziomie konkretnych powiatów.
#' @param bezrobocie ramka danych zwrócona przez wywołanie
#' `MLASZdane::pobierz_dane_bdl(MLASZdane::znajdz_wskazniki_bdl("stopa bezrobocia rejestrowanego"), zakresLat, "powiaty")`
#' @param wynagrodzenia ramka danych zwrócona przez wywołanie
#' `MLASZdane::pobierz_dane_bdl(MLASZdane::znajdz_wskazniki_bdl("przeciętne miesięczne wynagrodzenia brutto"), zakresLat, "powiaty")`
#' @param wynagrodzeniaWoj opcjonalnie ramka danych zwrócona przez wywołanie
#' `MLASZdane::pobierz_dane_bdl(MLASZdane::znajdz_wskazniki_bdl("przeciętne miesięczne wynagrodzenia brutto"), zakresLat, "województwa")`
#' @param zapiszDoPliku opcjonalnie ciąg znaków z nazwą pliku CSV, do którego ma
#' zostać zapisane przygotowane zestawienie; aby uniknąć zapisu do pliku, jako
#' wartość argumentu należy podać `NULL`
#' @returns ramka danych w formacie odpowiadającym specyfikacji
#' tabeli *wejściowej* W19; jeśli nastąpił zapis do pliku CSV, wynik zwracany
#' jest jako *ukryty*
#' @seealso [MLASZdane::pobierz_dane_bdl()], [MLASZdane::znajdz_wskazniki_bdl()]
#' @examples
#' \dontrun{
#' library(MLASZdane)
#' bezrobocie <- znajdz_wskazniki_bdl("stopa bezrobocia rejestrowanego") %>%
#'   pobierz_dane_bdl(2019:2024, "powiaty")
#' wynagrodzenia <- wskaznik_bdl(64428, "przeciętne miesięczne wynagrodzenia brutto") %>%
#'   pobierz_dane_bdl(2019:2024, "powiaty")
#' wynagrodzeniaWoj <- wskaznik_bdl(64428, "przeciętne miesięczne wynagrodzenia brutto") %>%
#'   pobierz_dane_bdl(2019:2024, "województwa")
#'
#' przygotuj_dane_do_w19(bezrobocie, wynagrodzenia, wynagrodzeniaWoj,
#'                       zapiszDoPliku = "W19.csv")
#' }
#' @importFrom utils write.table
#' @importFrom dplyr %>% .data arrange bind_rows case_match filter group_by lag
#'                   mutate reframe rename select semi_join ungroup
#' @importFrom tidyr pivot_wider
#' @export
przygotuj_dane_do_w19 <- function(bezrobocie, wynagrodzenia,
                                  wynagrodzeniaWoj = NULL,
                                  zapiszDoPliku = "W19.csv") {
  stopifnot(is.data.frame(bezrobocie),
            all(c("idWsk", "subjectId", "n1", "n2", "level", "measureUnitName",
                  "year", "idJst", "val", "teryt") %in% names(bezrobocie)),
            all(bezrobocie$idWsk %in% (461680L:461691L)),
            all(bezrobocie$n1 %in% c("styczeń", "luty", "marzec",
                                     "kwiecień", "maj", "czerwiec",
                                     "lipiec", "sierpień", "wrzesień",
                                     "październik", "listopad",
                                     "grudzień")),
            all(bezrobocie$n2 == "stopa bezrobocia rejestrowanego"),
            all(bezrobocie$subjectId == "P3559"),
            all(bezrobocie$level == 5L),
            all(bezrobocie$measureUnitName == "%"),
            is.numeric(bezrobocie$val),
            is.integer(bezrobocie$teryt), !anyNA(bezrobocie$teryt),
            is.data.frame(wynagrodzenia),
            all(c("idWsk", "subjectId", "n1", "level", "measureUnitName",
                  "year", "idJst", "val", "teryt") %in% names(wynagrodzenia)),
            all(wynagrodzenia$idWsk == 64428L),
            all(wynagrodzenia$n2 == "przeciętne miesięczne wynagrodzenia brutto"),
            all(wynagrodzenia$subjectId == "P2497"),
            all(wynagrodzenia$level == 5L),
            all(wynagrodzenia$measureUnitName == "zł"),
            is.numeric(wynagrodzenia$val),
            is.integer(wynagrodzenia$teryt), !anyNA(wynagrodzenia$teryt))
  stopifnot(is.null(wynagrodzeniaWoj) | is.data.frame(wynagrodzeniaWoj),
            is.null(zapiszDoPliku) | is.character(zapiszDoPliku))
  if (!is.null(zapiszDoPliku)) {
    stopifnot(length(zapiszDoPliku) == 1L, !anyNA(zapiszDoPliku))
  }
  if (!is.null(wynagrodzeniaWoj)) {
    stopifnot(all(c("idWsk", "subjectId", "n1", "level", "measureUnitName",
                    "year", "idJst", "val", "teryt") %in% names(wynagrodzeniaWoj)),
              all(wynagrodzeniaWoj$idWsk == 64428L),
              all(wynagrodzeniaWoj$n2 == "przeciętne miesięczne wynagrodzenia brutto"),
              all(wynagrodzeniaWoj$subjectId == "P2497"),
              all(wynagrodzeniaWoj$level == 5L),
              all(wynagrodzeniaWoj$measureUnitName == "zł"),
              is.numeric(wynagrodzeniaWoj$val),
              is.integer(wynagrodzeniaWoj$teryt), !anyNA(wynagrodzeniaWoj$teryt))
  }

  bezrobocie <- bezrobocie %>%
    mutate(MIESIAC = as.numeric(factor(.data$n1,
                                       c("styczeń", "luty", "marzec",
                                         "kwiecień", "maj", "czerwiec",
                                         "lipiec", "sierpień", "wrzesień",
                                         "październik", "listopad",
                                         "grudzień")))) %>%
    select(wskaznik = "n2", ROK = "year", "MIESIAC", TERYT = "teryt", "val")
  wynagrodzenia <- wynagrodzenia %>%
    rename(wskaznik = "n1", ROK = "year", TERYT = "teryt") %>%
    group_by(.data$wskaznik, .data$ROK, .data$TERYT) %>%
    reframe(MIESIAC = 1L:12L,
            val = rep(.data$val, 12L)) %>%
    mutate(val = ifelse(.data$val %in% 0, NA_real_, .data$val))
  wskaznikiBdl <- bind_rows(bezrobocie, wynagrodzenia) %>%
    mutate(wskaznik = case_match(
      .data$wskaznik,
      "stopa bezrobocia rejestrowanego" ~ "STOPA_BEZROBOCIA",
      "przeciętne miesięczne wynagrodzenia brutto" ~ "SR_WYNAGRODZENIA")) %>%
    pivot_wider(names_from = "wskaznik", values_from = "val") %>%
    arrange(.data$TERYT, .data$ROK, .data$MIESIAC)

  brakiWskazniki <- przygotuj_zestawienie_brakow_danych(wskaznikiBdl, "wskazniki")
  if (nrow(brakiWskazniki) > 0L) {
    cat(przygotuj_komunikat_o_brakach_wskazniki(brakiWskazniki))
  }
  brakiPowiaty <- przygotuj_zestawienie_brakow_danych(wskaznikiBdl, "powiaty")
  if (nrow(filter(brakiPowiaty, .data$wskaznik != "SR_WYNAGRODZENIA")) > 0L) {
    stop("Dane zawierają braki dla niektórych powiatów dla wskaźnika bezrobocia - to nie powinno się zdarzyć, więc sprawdź dane.")
  } else if (nrow(brakiPowiaty) > 0) {
    cat(przygotuj_komunikat_o_brakach_powiaty(brakiPowiaty, poImputacji = FALSE))
    if (!is.null(wynagrodzeniaWoj)) {
      cat("Zostanie podjęta próba zaimputowania tych braków z wykorzystaniem informacji o zmianach średnich wynagrodzeń w województwach.\n")
      wynagrodzeniaWoj <- wynagrodzeniaWoj %>%
        select(ROK = "year", TERYT_WOJ = "teryt", SR_WYNAGRODZENIA_WOJ = "val") %>%
        mutate(ZMIANA = .data$SR_WYNAGRODZENIA_WOJ /
                 lag(.data$SR_WYNAGRODZENIA_WOJ, n = 1))
      brakiPowiaty <- wskaznikiBdl %>%
        select("ROK", "TERYT", "SR_WYNAGRODZENIA") %>%
        distinct() %>%
        semi_join(brakiPowiaty,
                  by = "TERYT") %>%
        mutate(TERYT_WOJ = 10000 * floor(.data$TERYT / 10000)) %>%
        left_join(wynagrodzeniaWoj,
                  by = c("ROK", "TERYT_WOJ")) %>%
        group_by(.data$TERYT) %>%
        mutate(SR_WYNAGRODZENIA_IMPUT =
                 imputuj_wynagrodzenia(.data$SR_WYNAGRODZENIA, .data$ZMIANA)) %>%
        ungroup() %>%
        select("ROK", "TERYT", "SR_WYNAGRODZENIA_IMPUT")
      wskaznikiBdl <- wskaznikiBdl %>%
        left_join(brakiPowiaty,
                  by = c("ROK", "TERYT")) %>%
        mutate(SR_WYNAGRODZENIA = ifelse(is.na(.data$SR_WYNAGRODZENIA),
                                         .data$SR_WYNAGRODZENIA_IMPUT,
                                         .data$SR_WYNAGRODZENIA)) %>%
        select(-"SR_WYNAGRODZENIA_IMPUT")
      brakiPowiaty <- przygotuj_zestawienie_brakow_danych(wskaznikiBdl, "powiaty")
      if (nrow(brakiPowiaty) > 0L) {
        cat(przygotuj_komunikat_o_brakach_powiaty(brakiPowiaty,
                                                  poImputacji = TRUE))
      } else {
        cat("Udało się zaimputować wszystkie braki danych.\n")
      }
    } else {
      warning("Przygotowane dane zawierają braki danych wskaźnika wynagrodzeń w niektórych powiatach.")
    }
  }
  if (!is.null(zapiszDoPliku)) {
    zapiszDoPliku <- paste0(sub("\\.csv$", "", zapiszDoPliku), ".csv")
    write.table(wskaznikiBdl, zapiszDoPliku, sep = ";", dec = ".",
                col.names = TRUE, row.names = FALSE, na = "")
    invisible(wskaznikiBdl)
  } else {
    return(wskaznikiBdl)
  }
}
#' @title Funkcje pomocnicze - przygotowanie danych do tabeli wejsciowej W19
#' @description Nieeksportowana funkcja wykorzystywana wewnętrznie przez
#' [przygotuj_dane_do_w19()] - przygotowuje zestawienie braków wartości
#' wskaźników bezrobocia i przeciętnych wynagrodzeń.
#' @param wskazniki ramka danych w formacie odpowiadającym specyfikacji
#' *tabeli wejściowej* W19
#' @param co ciąg znaków wskazujący, czy ma zostać przygotowane zestawienie
#' braków danych opisujące, dla jakich lat i miesięcy brak wartości
#' poszczególnych wskaźników dla wszystkich powiatów w przekazanych danych
#' ("wskazniki") lub dla jakich poszczególnych powiatów brak wartości
#' wskaźników, choć dla innych powiatów w tym samym roku i miesiącu są one
#' dostępne w przekazanych danych ("powiaty")
#' @returns ramka danych z zestawieniem brakujących wartości wskaźników
#' @importFrom dplyr %>% distinct filter group_by mutate n semi_join summarise
#' @importFrom tidyr pivot_longer
przygotuj_zestawienie_brakow_danych <- function(wskazniki,
                                                co = c("wskazniki", "powiaty")) {
  co <- match.arg(co, several.ok = FALSE)
  brakiDanych <- wskazniki %>%
    pivot_longer(cols = c("STOPA_BEZROBOCIA", "SR_WYNAGRODZENIA"),
                 names_to = "wskaznik", values_to = "wartosc") %>%
    mutate(MIESIAC = ifelse(.data$wskaznik == "SR_WYNAGRODZENIA",
                            "cały rok", .data$MIESIAC)) %>%
    distinct() %>%
    group_by(.data$ROK, .data$MIESIAC, .data$wskaznik) %>%
    summarise(n = sum(is.na(.data$wartosc)),
              pct = sum(is.na(.data$wartosc)) / n(),
              .groups = "drop") %>%
    filter(.data$n > 0)
  if (co == "wskazniki") {
    return(brakiDanych %>%
             filter(.data$pct == 1))
  } else {
    return(wskazniki %>%
             pivot_longer(cols = c("STOPA_BEZROBOCIA", "SR_WYNAGRODZENIA"),
                          names_to = "wskaznik", values_to = "wartosc") %>%
             mutate(MIESIAC = ifelse(.data$wskaznik == "SR_WYNAGRODZENIA",
                                     "cały rok", .data$MIESIAC)) %>%
             distinct() %>%
             semi_join(brakiDanych %>%
                         filter(.data$pct < 1),
                       by = c("ROK", "MIESIAC", "wskaznik")) %>%
             filter(is.na(.data$wartosc)))
  }
}
#' @title Funkcje pomocnicze - przygotowanie danych do tabeli wejsciowej W19
#' @description Nieeksportowana funkcja wykorzystywana wewnętrznie przez
#' [przygotuj_dane_do_w19()] - przygotowuje komunikat o brakach wartości
#' wskaźników bezrobocia i przeciętnych wynagrodzeń, które dotyczą wszystkich
#' powiatów.
#' @param brakiWskazniki ramka danych zwrócona przez wywołanie
#' [przygotuj_zestawienie_brakow_danych()] z argumentem `co="wskazniki"`
#' @returns ciąg znaków z komunikatem
#' @importFrom dplyr %>% group_by pull summarise
przygotuj_komunikat_o_brakach_wskazniki <- function(brakiWskazniki) {
  return(paste(
    "W danych brak wartości wskaźników dla wszystkich (występujących w przekazanych danych) jednostek w następującym zakresie:\n",
    brakiWskazniki %>%
      group_by(.data$wskaznik, .data$ROK) %>%
      summarise(komunikat = paste0("  - w ", .data$ROK[1], " r. w miesiącach: ",
                                   paste(sort(.data$MIESIAC), collapse = ", ")),
                .groups = "drop_last") %>%
      summarise(komunikat = paste0("- Dla wskaźnika ", .data$wskaznik[1], ":\n",
                                   paste(.data$komunikat, collapse = ";\n"), ";")) %>%
      pull("komunikat") %>%
      paste(collapse = "\n"),
    "\n\n", sep = ""))
}
#' @title Funkcje pomocnicze - przygotowanie danych do tabeli wejsciowej W19
#' @description Nieeksportowana funkcja wykorzystywana wewnętrznie przez
#' [przygotuj_dane_do_w19()] - przygotowuje komunikat o brakach wartości
#' wskaźników bezrobocia i przeciętnych wynagrodzeń, które dotyczą
#' poszczególnych powiatów.
#' @param brakiPowiaty ramka danych zwrócona przez wywołanie
#' [przygotuj_zestawienie_brakow_danych()] z argumentem `co="powiaty"`
#' @param poImputacji wartość logiczna wskazująca, czy komunikat ma być
#' wyświetlony już po dokonaniu próby imputacji braków wartości wskaźników
#' @returns ciąg znaków z komunikatem
#' @importFrom dplyr %>% group_by pull summarise
przygotuj_komunikat_o_brakach_powiaty <- function(brakiPowiaty,
                                                  poImputacji = FALSE) {
  return(paste(
    ifelse(poImputacji, "Po przeprowadzeniu imputacji w", "W"),
    " danych występują braki danych wskaźnika średnich wynagrodzeń na poziomie powiatów:\n",
    brakiPowiaty %>%
      group_by(.data$wskaznik, .data$TERYT) %>%
      summarise(komunikat = paste0("  - w latach: ",
                                   paste(sort(.data$ROK), collapse = ", ")),
                .groups = "drop_last") %>%
      summarise(komunikat = paste0("- Dla powiatu ", .data$TERYT[1], ":\n",
                                   paste(.data$komunikat, collapse = ";\n"), ";")) %>%
      pull("komunikat") %>%
      paste(collapse = "\n"),
    "\n\n", sep = ""))
}
#' @title Funkcje pomocnicze - przygotowanie danych do tabeli wejsciowej W19
#' @description Nieeksportowana funkcja wykorzystywana wewnętrznie przez
#' [przygotuj_dane_do_w19()] - przeprowadza imputację wartości wskaźnika na
#' podstawie informacji o zmianach przeciętnego poziomu wynagrodzeń.
#' @param w wektor liczb - z założenia są to wartości przeciętnych miesięcznych
#' wynagrodzeń w powiecie w kolejnych okresach (latach); może zawierać braki
#' danych
#' @param zmiany wektor liczb - z założenia są to ilorazy przeciętnych
#' miesięcznych wynagrodzeń w danym okresie (roku) do przeciętnych miesięcznych
#' wynagrodzeń w poprzednim okresie (roku) w województwie, w którym znajduje się
#' dany powiat
#' @details
#' Imputacja przeprowadzana jest krokowo i **wymaga, aby pierwsza wartość `w`
#' nie była brakiem danych**. Przebiega w ten sposób, że dla każdego elementu
#' `w`, począwszy od drugiego, jeśli jest on brakiem danych, to podstawia się
#' za niego wartość iloczynu poprzedniego elementu `w` i danego elementu
#' `zmiany` (tj. arbitralnie zakłada się, że przeciętne zarobki w danym powiecie
#' zmieniły się procentowo względem poprzedniego roku o tyle samo, co w całym
#' województwie).
#' @returns wektor liczb: `w` z dokonanymi imputacjami
imputuj_wynagrodzenia <- function(w, zmiany) {
  stopifnot(is.numeric(w), is.numeric(zmiany),
            length(w) == length(zmiany))
  for (i in seq_along(w)[-1]) {
    if (is.na(w[i])) w[i] <- w[i - 1] * zmiany[i]
  }
  return(w)
}
