#' @title Obliczanie "okresu" na podstawie roku i miesiąca
#' @description Funkcja pomocnicza pozwalająca obliczyć numer "okresu" na
#' na podstawie podanych lat i miesięcy.
#' @param rok wektor liczb całkowitych opisujących lata
#' @param miesiac wektor liczb całkowitych ze zbioru \{1, ..., 12\} opisujących
#' miesiące
#' @param warnIfNAs wartość logiczna - czy generować ostrzeżenie, jeśli argument
#' `x` zawiera braki danych?
#' @returns wektor liczb całkowitych
#' @seealso [okres2rokmiesiac()], [okres2rokszk()], [okres2data()],
#' [data2rokszk()]
#' @examples
#' rokmiesiac2okres(c(2023, 2024), c(6, 12))
#' rokmiesiac2okres(c(2023, 2024), 12)
#' @export
rokmiesiac2okres <- function(rok, miesiac, warnIfNAs = FALSE) {
  stopifnot(is.numeric(rok),
            all(as.integer(rok) == rok | is.na(rok)),
            all(is.finite(rok) | is.na(rok)),
            all(miesiac %in% (1:12) | is.na(miesiac)))
  if ((anyNA(rok) | anyNA(miesiac)) & warnIfNAs) {
    warning("W przekazanych wektorach występują braki danych.")
  }
  return(12*rok + miesiac)
}
#' @title Zamiana "okresu" na rok i miesiac
#' @description Funkcja pomocnicza pozwalająca zamienić numer "okresu", używany
#' jako identyfikator miesiąca w tabelach *pośrednich* na odpowiadający mu
#' rok i numer miesiąca (w ramach roku).
#' @param x liczba całkowita (co do zasady wartość kolumny `okres`)
#' @param SIMPLIFY wartość logiczna - jeśli argument `x` jest pojedynczą liczbą
#' (wektorem jednoelementowym), to czy zwrócony ma być dwuelementowy wektor
#' liczb zamiast listy?
#' @param warnIfNAs wartość logiczna - czy generować ostrzeżenie, jeśli argument
#' `x` zawiera braki danych?
#' @return lista złożona z dwóch elementów: `rok` i `miesiac`, z których każdy
#' jest wektorem liczb całkowitych lub dwuelementowy wektor liczb całkowitych
#' (jeśli argument `SIMPLIFY= TRUE`, a argument `x` jest długości 1)
#' @seealso [rokmiesiac2okres()], [okres2rokszk()], [okres2data()],
#' [data2rokszk()]
#' @examples
#' okres2rokmiesiac(24249)
#' okres2rokmiesiac(c(24249, 24252, 24253))
#' @export
okres2rokmiesiac <- function(x, SIMPLIFY = TRUE, warnIfNAs = TRUE) {
  stopifnot(is.numeric(x), all(as.integer(x) == x),
            all(is.finite(x) | is.na(x)),
            is.logical(SIMPLIFY), length(SIMPLIFY) == 1L, !anyNA(SIMPLIFY),
            is.logical(warnIfNAs), length(warnIfNAs) == 1L, !anyNA(warnIfNAs))
  if (anyNA(x) & warnIfNAs) {
    warning("Przekazany wektor zawiera braki danych.")
  }
  rok <- floor((x - 1) / 12)
  miesiac <- x - 12L*(floor((x - 1) / 12))
  if (length(x) == 1L & SIMPLIFY) {
    return(c(rok = rok, miesiac = miesiac))
  } else {
    return(list(rok = rok, miesiac = miesiac))
  }
}
#' @title Zamiana "okresu" na date w formacie "%d-%m-%Y"
#' @description Funkcja pomocnicza pozwalająca zamienić numer "okresu", używany
#' jako identyfikator miesiąca w tabelach *pośrednich* na odpowiadającą mu
#' ciąg znaków opisujący datę w formacie "%d-%m-%Y".
#' @inheritParams okres2rokmiesiac
#' @param dzien opcjonalnie liczba całkowita lub ciąg znaków dający się
#' skonwertować na liczbę całkowitą opisująca dzień miesiąca, który zostanie
#' przypisany tworzonym datom; może być to też wektor takich liczb lub ciągów
#' znaków, o liczbie elementów równej liczbie elementów argumentu `x`, co
#' pozwala przypisać różne dni różnym okresom w ramach `x`
#' @return wektor ciągów znaków z datami zapisanymi w formacie "%d-%m-%Y"
#' @seealso [okres2rokmiesiac()], [okres2rokszk()], [rokmiesiac2okres()],
#' [data2rokszk()]
#' @examples
#' okres2data(24249)
#' okres2data(c(24249, 24252, 24253))
#' @export
okres2data <- function(x, dzien = "01", warnIfNAs = TRUE) {
  x <- okres2rokmiesiac(x, SIMPLIFY = FALSE, warnIfNAs = warnIfNAs)
  stopifnot(is.character(dzien) | is.numeric(dzien),
            length(dzien) == 1L | length(dzien) == length(x),
            all(as.integer(dzien) %in% (1:31)))
 return(paste(dzien, x$miesiac, x$rok, sep = "-"))
}
#' @title Zamiana "okresu" na rok szkolny
#' @description Funkcja pomocnicza pozwalająca zamienić numer "okresu", używany
#' jako identyfikator miesiąca w tabelach *pośrednich* na odpowiadający mu
#' rok szkolny (jako liczbę wskazującą **pierwszy** rok kalendarzowy w ramach
#' odpowiedniego roku szkolnego albo tekst z pełnym opisem roku szkolnego).
#' @inheritParams okres2rokmiesiac
#' @param tekst wartość logiczna - czy zwrócony powinien zostać tekst z pełnym
#' opisem roku szkolnego (np. "2023/2024"), czy liczba całkowita wskazującą
#' **pierwszy** rok kalendarzowy w ramach odpowiedniego roku szkolnego
#' @return wektor ciągów znaków lub wektor liczb całkowitych
#' @seealso [data2rokszk()], [okres2rokmiesiac()], [okres2data()],
#' [rokmiesiac2okres()]
#' @examples
#' okres2rokszk(c(24248, 24249))
#' okres2rokszk(c(24248, 24249), tekst = FALSE)
#' @export
okres2rokszk <- function(x, tekst = TRUE, warnIfNAs = TRUE) {
  stopifnot(is.logical(tekst), length(tekst) == 1L, !anyNA(tekst))
  x <- okres2rokmiesiac(x)
  rok <- x$rok - ifelse(x$miesiac %in% (1L:8L), 1L, 0L)
  if (tekst) rok <- paste0(rok, "/", rok + 1L)
  return(rok)
}
#' @title Zamiana daty w formacie "%d-%m-%Y" na rok szkolny
#' @description Funkcja pomocnicza pozwalająca zamienić datę w formacie
#' "%d-%m-%Y" na rok szkolny (jako liczbę wskazującą **pierwszy** rok
#' kalendarzowy w ramach odpowiedniego roku szkolnego albo tekst z pełnym opisem
#' roku szkolnego).
#' @param x wektor ciągów znaków zawierający daty w formacie
#' @param max liczba - wartość odpowiadająca **pierwszemu** rokowi roku
#' szkolnego, który zostanie przypisana datom opisującym niezakończony okres
#' nauki (tj. "31-12-9999")
#' @param tekst wartość logiczna - czy zwrócony powinien zostać tekst z pełnym
#' opisem roku szkolnego (np. "2023/2024"), czy liczba całkowita wskazującą
#' **pierwszy** rok kalendarzowy w ramach odpowiedniego roku szkolnego
#' @return wektor ciągów znaków lub wektor liczb całkowitych
#' @seealso [okres2rokmiesiac()], [okres2rokszk()], [okres2data()],
#' [rokmiesiac2okres()]
#' @examples
#' data2rokszk(c("30-08-2023", "01-09-2023", "31-12-9999"),
#'             max = 2024)
#' data2rokszk(c("30-08-2023", "01-09-2023", "31-12-9999"),
#'             max = 2024, tekst = FALSE)
#' @export
data2rokszk <- function(x, max, tekst = TRUE) {
  stopifnot(is.character(x), !anyNA(x),
            is.numeric(max), length(max) == 1L, !anyNA(max), max >= 2020,
            is.finite(max), as.integer(max) == max,
            is.logical(tekst), length(tekst) == 1L, !anyNA(tekst))
  x <- as.Date(x, format = "%d-%m-%Y")
  stopifnot("Niektóre elementy wektora przekazanego argumentem `x` nie są poprawnymi datami w formacie '%d-%m-%Y'" = !anyNA(x))
  rok <- pmin(as.integer(format(x, format = "%Y")), max) -
    ifelse(as.integer(format(x, format = "%m")) %in% (1L:8L),
           1L, 0L)
  if (tekst) rok <- paste0(rok, "/", rok + 1L)
  return(rok)
}
