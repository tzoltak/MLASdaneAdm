#' @title Przycinanie wartosci odstajacych oszacowan wynagrodzen
#' @description Funkcja przycina wartości odstające zmiennych - typowo
#' opisujących wynagrodzenia. Progi przycięcia obliczane są jako kwantyle zadane
#' argumentem `q` w odniesieniu do zmiennej o nazwie podaje argumentem
#' `zmKryterium`, oddzielnie w ramach każdej z podgrup wyróżnionych przez
#' przecięcie ze sobą wartości zmiennych, których nazwy zostały podane
#' argumentem `zmGrupujące`. Następnie wszystkie wartości zmiennych, których,
#' nazwy zostały podane argumentami `zmKryterium` lub `zmZastosuj`
#' przekształcane są tak, że wartości na zewnątrz od obliczonych progów
#' zamieniane są na wartości progów.
#' @param dane ramka danych, typowo tabela *pośrednia* P3
#' @param zmKryterium ciąch znaków - nazwa zmiennej, na podstawie rozkładu
#' której mają zostać obliczone progi
#' @param zmZastosuj opcjonalnie wektor ciągów znaków - nazwy zmiennych, których
#' wartości mają zostać przycięte (oprócz zmiennej podanej argumentem
#' `zmKryterium`, której wartości są przycinane zawsze)
#' @param zmGrupujace opcjonalnie wektor ciągów znaków - nazwy zmiennych,
#' przecięcie wartości których definiuje grupy, w ramach których mają zostać
#' obliczone progi
#' @param q opcjonalnie dwuelamentowy wektor liczbowy z wartościami kwantyli,
#' definiującymi progi odcięcia (domyślnie 0.005 i 0.995); jeśli zostanie podana
#' tylko jedna wartość, drugi kwantyl zostanie dobrany automatycznie, jako
#' jej dopełnienie do 1
#' @param usun0 opcjonalnie wartość logiczna - czy przed obliczeniemn kwantyli
#' oraz dokonaniem przycięcia wartości przekształcanych zmiennych równe 0
#' powinny zostać zamienione na braki danych? (domyślnie `TRUE`)
#' @param zachowajOryginalne opcjonalnie wartość logiczna - czy w danych
#' powinny zostać zachowane oryginalne, nieprzycięte kopie przekształcanych
#' zmiennych? (domyślnie `TRUE`)
#' **kopie takie otrzymają w nazwie przyrostek "_oryg"**
#' @return ramka danych przekazana argumentem `dane` zawierająca przekształcone
#' zmienne i ew. te same zmienne nieprzekształcone, ale z dodanym do nazwy
#' przyrostkiem "o_ryg"
#' @importFrom stats quantile
#' @importFrom dplyr %>% across all_of case_when group_by left_join mutate n
#'             select summarise ungroup
#' @export
przytnij_wynagrodzenia <- function(dane, zmKryterium,
                                   zmZastosuj = vector(mode = "character",
                                                       length = 0),
                                   zmGrupujace = vector(mode = "character",
                                                        length = 0),
                                   q = c(0.005, 0.995),
                                   usun0 = TRUE, zachowajOryginalne = TRUE) {
  stopifnot(is.data.frame(dane),
            is.character(zmKryterium), length(zmKryterium) == 1,
            !anyNA(zmKryterium), zmKryterium %in% names(dane),
            is.character(zmZastosuj), !anyNA(zmZastosuj),
            all(zmZastosuj %in% names(dane)),
            is.character(zmGrupujace), !anyNA(zmGrupujace),
            all(zmGrupujace %in% names(dane)),
            sapply(dane[, c(zmKryterium, zmZastosuj)], is.numeric),
            is.numeric(q), length(q) %in% c(1, 2), !anyNA(q),
            all(q > 0), all(q < 1),
            is.logical(usun0), length(usun0) == 1,
            usun0 %in% c(TRUE, FALSE),
            is.logical(zachowajOryginalne), length(zachowajOryginalne) == 1,
            zachowajOryginalne %in% c(TRUE, FALSE))
  zmZastosuj <- union(zmKryterium, zmZastosuj)
  if (length(q ) == 1) {
    q[2] <- sort(q, 1 - q)
  } else {
    q <- sort(q)
  }

  if (zachowajOryginalne) {
    dane <- dane %>%
      mutate(across(all_of(zmZastosuj),
                    list(oryg = ~.)))
  }
  if (usun0) {
    dane <- dane %>%
      mutate(across(all_of(zmZastosuj),
                    ~ifelse(. %in% 0, NA_real_, .)))
  }
  progi <- dane %>%
    group_by(across(all_of(zmGrupujace))) %>%
    summarise(across(all_of(zmKryterium),
                     list(`__q1` = ~quantile(., q[1], na.rm = TRUE,
                                             names = FALSE),
                          `__q2` = ~quantile(., q[2], na.rm = TRUE,
                                             names = FALSE))),
              .groups = "drop")
  zmProgi <- setdiff(names(progi), zmGrupujace)
  stopifnot(!any(zmProgi %in% names(dane)))

  dane %>%
    left_join(progi,
              by = zmGrupujace) %>%
    group_by(across(all_of(zmGrupujace))) %>%
    mutate(across(all_of(zmZastosuj),
                  ~case_when(. < .data[[zmProgi[[1]]]] ~ .data[[zmProgi[1]]],
                             . > .data[[zmProgi[[2]]]] ~ .data[[zmProgi[2]]],
                             .default = .))) %>%
    ungroup() %>%
    select(-all_of(zmProgi)) %>%
    return()
}
