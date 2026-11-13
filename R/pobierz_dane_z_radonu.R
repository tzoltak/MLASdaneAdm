#' @title Przygotowanie danych do tabeli wejsciowej W33
#' @description Funkcja pobiera z API RAD-on informacje o kierunkach studiów
#' i dołącza je do po kombinacji wartości identyfikatora uczelni/filii uczelni,
#' profilu studiów, nazwie kierunku i formie studiów do unikalnych kombinacji
#' wartości tychże zmiennych występujących w pliku wejściowym *W12.csv*.
#'
#' **Niestety, w większości wypadków relacja (unikalne kombinacje z *W12.csv*)
#' do RAD-on okazuje się mieć formę 1:n**. W szczególności studia na różnych
#' poziomach często nazywają się dokładnie tak samo, **co czyni efekty takiego
#' łączenia raczej bezużytecznymi**. Niemniej funkcja umożliwia przynajmniej
#' łatwe zrekonsstruowanie tego problemu.
#' @param tryb ciąg znaków opisujący, czego funkcja ma szukać w RAD-onie:
#' "uczelnioProfile" określa unikalne kombinacje identyfikatora uczelni
#' i profilu studiów i odpytuje RAD-on o wszystkie kierunki pasujące do każdej
#' takiej kombinacji; "kierunki" określa unikalne kombinacji identyfikatora
#' uczelni, nazwy kierunku i profilu studiów i odpytuje o nie RAD-on (problem
#' może tu polegać na tym, że *W12.csv* mamy raczej nazwy tzw. *uruchomień*,
#' a nie nazwy kierunków, w sensie, w jakim posługuje się nią POL-on/RAD-on)
#' @returns ramka danych
#' @details
#' API RAD-on przy odpytywaniu go o kierunki z wykorzystaniem ich nazw
#' 1) szuka po nazwach kierunków, **nie** po nazwach *uruchomień*, ale 2) zwraca
#' choćby częściowe dopasowania i 3) ignoruje wielkość liter.
#'
#' Nazwy kierunków w danych monitoringu miewają inną konwencję zapisu wielkimi
#' literami, niż te pobierane z API RAD-on - nie da się niestety jednoznacznie
#' stwierdzić, czy to różnica znacząca, czy nie, ale fakt, że w danych z API
#' RAD-onu w ogóle nie występują niektóre kombinacje identyfikatora uczelni
#' i nazwy kierunku (pasowane z uwzględnieniem wielkości liter), jakie mamy
#' w danych z OPI, sugeruje, że niekoniecznie. W związku z tym w trybie
#' "uczelnioProfile" podczas przyłączania danych pobranych z RAD-onu do tych
#' z pliku *W12.csv*, porównanie nazw kierunków dokonywane jest z pominięciem
#' wielkości znaków (technicznie: po zamianie nazw kierunków na pisane saymi
#' wielkimi litereami). Może to jednak skutkować jakimiś fałyszywmi
#' przypisaniami.
#' @seealso [pobierz_kierunki_uczelni_profilu()]
#' @importFrom utils txtProgressBar setTxtProgressBar
#' @importFrom dplyr %>% .data add_count count distinct filter left_join mutate rename select
#' @importFrom tidyr unnest
#' @export
pobierz_dane_z_radonu <- function(tryb = c("uczelnioProfile", "kierunki")) {
  tryb = match.arg(tryb, several.ok = FALSE)
  stopifnot("W katalogu roboczym nie ma pliku 'W12.csv'." =
              file.exists("W12.csv"))

  message("Wczytywanie pliku 'W12.csv'")
  w12 <- arrow::read_csv2_arrow("W12.csv")
  stopifnot(all(c("ID_PODMIOTU", "UCZELNIA_STU", "KIERUNEK_STU", "PROFIL_STU",
                  "FORMA_STU") %in% names(w12)))
  w12 <- w12 %>%
    filter(!(.data$ID_PODMIOTU %in% c("", NA_character_))) %>%
    count(.data$ID_PODMIOTU, .data$UCZELNIA_STU, .data$KIERUNEK_STU,
          .data$PROFIL_STU, .data$FORMA_STU, name = "lAbs") %>%
    filter(.data$KIERUNEK_STU != "Na pierwszym roku studiów bez przypisanego kierunku") %>%
    mutate(profileCode = c(O = 2L, P = 1L)[.data$PROFIL_STU],
           profileName = c(O = "ogólnoakademicki",
                           P = "praktyczny")[.data$PROFIL_STU],
           formName = c(N = "niestacjonarne",
                        S = "stacjonarne")[.data$FORMA_STU])
  message("...zakończone.")

  if (tryb == "uczelnioProfile") {
    message("Pobieranie danych o kierunkach z API RAD-on:")
    uczelnioProfile <- w12 %>%
      count(.data$ID_PODMIOTU, .data$profileCode, name = "lKierunkow") %>%
      rename(leadingInstitutionUuid = "ID_PODMIOTU") %>%
      mutate(kierunki = vector(mode = "list", length = n()))
    pb <- txtProgressBar(0, nrow(uczelnioProfile), style = 3)
    for (up in seq_len(nrow(uczelnioProfile))) {
      uczelnioProfile$kierunki[[up]] <-
        pobierz_kierunki_uczelni_profilu(uczelnioProfile$leadingInstitutionUuid[up],
                                         uczelnioProfile$profileCode[up])
      setTxtProgressBar(pb, up)
    }
    close(pb)
    message("Zakończone.")

    uczelnioProfile2 <- uczelnioProfile %>%
      mutate(kierunki =
               lapply(.data$kierunki,
                      \(x) {
                        if (ncol(x) == 0 || nrow(x) == 0) return(data.frame(nic = NA)[, c()])
                        return(x %>%
                                 select("courseUuid", "courseName", "iscedName",
                                        courseInstanceUuid = "courseInstances_courseInstanceUuid",
                                        "courseInstances_courseName",
                                        formName = "courseInstances_formName",
                                        languageName = "courseInstances_languageName",
                                        numberOfSemesters = "courseInstances_numberOfSemesters",
                                        ects = "courseInstances_ects"))
                      })) %>%
      select(-"lKierunkow") %>%
      unnest("kierunki") %>%
      filter(!is.na(.data$courseUuid)) %>%
      rename(ID_PODMIOTU = "leadingInstitutionUuid") %>%
      mutate(KIERUNEK_STU_LACZ = toupper(.data$courseInstances_courseName),
             across(c(.data$numberOfSemesters, .data$ects), as.integer)) %>%
      distinct()
    w12 <- w12 %>%
      mutate(KIERUNEK_STU_LACZ = toupper(.data$KIERUNEK_STU)) %>%
      left_join(uczelnioProfile2,
                by = c("ID_PODMIOTU", "KIERUNEK_STU_LACZ", "profileCode", "formName")) %>%
      select(-c("profileCode", "profileName", "formName", "KIERUNEK_STU_LACZ")) %>%
      add_count(.data$ID_PODMIOTU, .data$KIERUNEK_STU, .data$PROFIL_STU,
                .data$FORMA_STU, name = "lRekordowRadon")
  } else {
    message("Pobieranie danych o kierunkach z API RAD-on:")
    kierunki <- w12 %>%
      count(.data$ID_PODMIOTU, .data$KIERUNEK_STU, .data$profileCode,
            name = "lKierunkow") %>%
      rename(leadingInstitutionUuid = "ID_PODMIOTU",
             courseName = "KIERUNEK_STU") %>%
      mutate(kierunki = vector(mode = "list", length = n()))
    pb <- txtProgressBar(0, nrow(kierunki), style = 3)
    for (up in seq_len(nrow(kierunki))) {
      wynik <- GET("https://radon.nauka.gov.pl/opendata/polon/courses",
                   query = list(leadingInstitutionUuid = kierunki$leadingInstitutionUuid[up],
                                courseName = kierunki$courseName[up],
                                profileCode = unname(kierunki$profileCode[up]),
                                resultNumbers = 100L),
                   add_headers(Accept = "application/json"))
      if (wynik$status_code != 200) {
        stop("Błąd pobierania danych. Kod HTTP: ", wynik$status_code)
      }
      wynik <- content(wynik, "text", encoding = "UTF-8") %>%
        fromJSON(flatten = TRUE)
      wynik <- wynik$results
      if (is.data.frame(wynik)) {
        wynik <- unnest(wynik, "courseInstances", names_sep = "_")
      } else {
        wynik <- data.frame()
      }
      kierunki$kierunki[[up]] <- wynik
      setTxtProgressBar(pb, up)
    }
    close(pb)
    message("Zakończone.")

    kierunki <- kierunki %>%
      mutate(kierunki =
               lapply(.data$kierunki,
                      \(x) {
                        if (ncol(x) == 0 || nrow(x) == 0) return(data.frame(nic = NA)[, c()])
                        return(x %>%
                                 select("courseUuid", "iscedName",
                                        courseInstanceUuid = "courseInstances_courseInstanceUuid",
                                        courseNameRADon = "courseName",
                                        "courseInstances_courseName",
                                        formName = "courseInstances_formName",
                                        languageName = "courseInstances_languageName",
                                        numberOfSemesters = "courseInstances_numberOfSemesters",
                                        ects = "courseInstances_ects"))
                      })) %>%
      select(-"lKierunkow") %>%
      unnest("kierunki") %>%
      filter(!is.na(.data$courseUuid),
             toupper(.data$courseName) == toupper(.data$courseNameRADon)) %>%
      rename(ID_PODMIOTU = "leadingInstitutionUuid",
             KIERUNEK_STU = "courseName") %>%
      mutate(across(c(.data$numberOfSemesters, .data$ects), as.integer)) %>%
      distinct()
    w12 <- w12 %>%
      left_join(kierunki,
                by = c("ID_PODMIOTU", "KIERUNEK_STU", "profileCode", "formName")) %>%
      select(-c("profileCode", "profileName", "formName", "courseNameRADon")) %>%
      add_count(.data$ID_PODMIOTU, .data$KIERUNEK_STU, .data$PROFIL_STU,
                .data$FORMA_STU, name = "lRekordowRadon")
  }

  return(w12)
}
#' @title Przygotowanie danych do tabeli wejsciowej W33
#' @description Funkcja pobiera z API RAD-on informacje o kierunkach studiów
#' na zadanej uczelni/filii, o zadanym profilu.
#' @param idPodmiotu ciąg znaków - unikalny identyfikator uczelni/filii uczelni
#' prowadzącej studia
#' @param kodProfilu liczba całkowita - kod profilu studiów
#' @returns ramka danych zawierająca pobrane a API RAD-onu informacje
#' o kierunkach studiów i ich *uruchomieniach*
#' @seealso [pobierz_dane_z_radonu()]
#' @importFrom httr add_headers content GET
#' @importFrom jsonlite fromJSON
#' @importFrom tidyr unnest
pobierz_kierunki_uczelni_profilu <- function(idPodmiotu, kodProfilu) {
  stopifnot(is.character(idPodmiotu), length(idPodmiotu) == 1L,
            !is.na(idPodmiotu), idPodmiotu != "",
            is.numeric(kodProfilu), length(kodProfilu) == 1L,
            !is.na(kodProfilu), as.integer(kodProfilu) == kodProfilu)
  url <- "https://radon.nauka.gov.pl/opendata/polon/courses"
  resultNumbers = 100L # więcej się nie da - ograniczenie API
  query <- list(leadingInstitutionUuid = idPodmiotu,
                profileCode = unname(kodProfilu))
  maxCount <- content(GET(url, query = c(query, list(resultNumbers = 1)),
                          add_headers(Accept = "application/json")))$pagination$maxCount
  query <- c(query, list(resultNumbers = 100L)) # więcej się nie da - ograniczenie API
  kierunki <- vector(mode = "list", length = maxCount %/% resultNumbers + 1L)
  token <- NULL
  for (i in seq_along(kierunki)) {
    if (!is.null(token)) query$token <- token
    wynik <- GET(url, query = query,
                 add_headers(Accept = "application/json"))
    if (wynik$status_code != 200) {
      stop("Błąd pobierania danych. Kod HTTP: ", wynik$status_code)
    }
    wynik <- content(wynik, "text", encoding = "UTF-8") %>%
      fromJSON(flatten = TRUE)

    kierunki[[i]] <- wynik$results
    token <- wynik$pagination$token # KLUCZOWE: token musi być przekazany dalej
  }
  kierunki <- bind_rows(kierunki)
  if (nrow(kierunki) > 0L) {
    kierunki <- unnest(kierunki, "courseInstances", names_sep = "_")
  }
  return(kierunki)
}
