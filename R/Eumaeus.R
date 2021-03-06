# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of Eumaeus
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' @keywords internal
"_PACKAGE"

#' @importFrom rlang .data
#' @importFrom methods is
#' @importFrom grDevices rgb
#' @importFrom stats aggregate coef confint density pnorm qnorm quantile dpois pbinom pchisq
#' @importFrom utils read.csv setTxtProgressBar txtProgressBar write.csv write.table installed.packages packageVersion
#' @import dplyr
#' @import DatabaseConnector
#' @import survival
NULL

cache <- new.env()
