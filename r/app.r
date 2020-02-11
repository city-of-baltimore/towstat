library(shiny)
library(lubridate)
library(dplyr)
library(googleCharts)
library(googleVis)

# Prepare data from the towing file
towingdata <- read.csv('towing.csv', header = TRUE)
towingdata$datetime <- lubridate::ymd(towingdata$datetime)
towingdata <- dplyr::arrange(towingdata, datetime)
datemax <- as.Date(towingdata[which.max(as.POSIXct(towingdata$datetime)), ]$datetime)
datemin <- as.Date(towingdata[which.min(as.POSIXct(towingdata$datetime)), ]$datetime)

# Prepare data from the categories file
pickupdata <- read.csv('pickups.csv')

# Prepare data from the oldest vehicle file
oldestvehicledata <- read.csv('oldest.csv')

ui <- fluidPage(
  # This line loads the Google Charts JS library
  googleChartsInit(),

  # Use the Google webfont "Source Sans Pro"
  tags$link(
    href=paste0("http://fonts.googleapis.com/css?",
                "family=Source+Sans+Pro:300,600,300italic"),
    rel="stylesheet", type="text/css"
  ),

  tags$style(type="text/css",
    "body {font-family: 'Source Sans Pro'}"
  ),

  tabsetPanel(
    tabPanel(
      "TowStat",
      sidebarPanel(
        checkboxInput("dirtbikequantitycb", "Include dirtbikes", value = FALSE, width = NULL),
        checkboxGroupInput(
          "vehiclequantitycb",
          h3("Quantity of Vehicles"),
          choices = list(
            "Overall" = "total_num",
            "Police Action" = "police_action_num",
            "Police Hold" = "police_hold_num",
            "Accident" = "accident_num",
            "Abandoned" = "abandoned_num",
            "Scofflaw" = "scofflaw_num",
            "Impound" = "impound_num",
            "Stolen-Recovered" = "stolen_recovered_num",
            "No code" = "nocode_num"
          ),
          selected = "total_num"
        ),
        checkboxGroupInput(
          "daysonlotcb",
          h3("Average days on lot"),
          choices = list(
            "Overall" = "total_avg",
            "Police Action" = "police_action_avg",
            "Police Hold" = "police_hold_avg",
            "Accident" = "accident_avg",
            "Abandoned" = "abandoned_avg",
            "Scofflaw" = "scofflaw_avg",
            "Impound" = "impound_avg",
            "Stolen-Recovered" = "stolen_recovered_avg",
            "No code" = "nocode_avg"
          ),
          selected = "total_avg"
        )
      ),
      mainPanel(
        h4("TowStat"),
        fluidRow(
         sliderInput(
           "date", "Date",
           min = datemin + 3650,
           max = datemax,
           value = c(datemax - 180,
                     datemax),
           animate = TRUE,
           width = '90%'
         )
        ),
        htmlOutput("quantityview",
                  width="90%"),
        htmlOutput("avgview",
                  width="90%"),
      )
    ),
    tabPanel(
      "Categories",
      sidebarPanel(
        checkboxGroupInput(
          "towcategoriescb",
          h3("Average days on lot"),
          choices = list(
            "Police Action" = "111",
            "Police Hold" = "1111",
            "Accident" = "112",
            "Abandoned" = "113",
            "Scofflaw" = "125",
            "Impound" = "140",
            "Stolen-Recovered" = "200",
            "No code" = "1000"
          ),
          selected = c("111", "1111", "112", "113", "125", "140", "200")
        ),
      ),
      mainPanel(
        h4("Tow categories of current vehicles on lot"),
        htmlOutput("towcategoriesplot",
                   width="90%")
      )
    ),
    tabPanel(
      "Oldest vehicles",
      h4("The oldest cars on the lot"),
      DT::dataTableOutput("oldestvehicles")
    )
  )
)


server <- function(input, output) {
  # Vehicle quanity graph on the TowStat tab
  output$quantityview <- renderGvis({
    quantity_fields <- reactive({
      if (input$dirtbikequantitycb){
        input$vehiclequantitycb
      } else{
        str_replace(input$vehiclequantitycb, "_num", "_nondb_num")
      }
    })
    
    data <- reactive({
      towingdata %>%
        select(c("datetime", quantity_fields())) %>%
        filter(as.Date(datetime) >= as.Date(input$date[1]) & as.Date(input$date[2]) >= as.Date(datetime))
    })
    gvisLineChart(
      data(),
      xvar='datetime',
      yvar=quantity_fields(),
      options=list(
        legend="{ position: 'right', maxLines: 3 }",
        vAxes="[{title:'quantity'}]",
        width="100%",
        height=350
      )
    )
  })

  # Vehicle age graph on the TowStat tab
  output$avgview <- renderGvis({
    age_fields <- reactive({
      if (input$dirtbikequantitycb){
        input$daysonlotcb
      } else{
        str_replace(input$daysonlotcb, "_avg", "_nondb_avg")
      }
    })
    data <- reactive({
      towingdata %>%
        select(c("datetime", age_fields())) %>%
        filter(as.Date(datetime) >= as.Date(input$date[1]) & as.Date(input$date[2]) >= as.Date(datetime))
    })
    gvisLineChart(
      data(),
      xvar='datetime',
      yvar=age_fields(),
      options=list(
        legend="{ position: 'right', maxLines: 3 }",
        vAxes="[{title:'avg age'}]",
        width="100%",
        height=350
      )
    )
  })

  # Categories graph on the categories tab
  output$towcategoriesplot <- renderGvis({
    data <- reactive({
      subset(pickupdata, base_pickup_type %in% input$towcategoriescb, select = c('pickup_type', 'with.dirtbikes', 'without.dirtbikes'))
    })
    gvisBarChart(
      data(),
      xvar='pickup_type',
      yvar=c('with.dirtbikes', 'without.dirtbikes'),
      options=list(
        hAxis="{title: '# of vehicles'}",
        vAxis="{title: 'Categories'}",
        height=500,
        legend="{position: 'top'}"
      )
    )
  })

  # DataTable of the oldest vehicles on the oldest vehicles tab
  output$oldestvehicles = DT::renderDataTable({
    oldestvehicledata
  })
}

shinyApp(ui, server)
