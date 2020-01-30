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

# Prepare data from the top 15 file
top15data <- read.csv('top_15.csv')

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

  xlim <- list(
    min = datemin - 10,
    max = datemax + 10
  ),

  ylim <- list(
    min = min(towingdata$number) - 5,
    max = max(towingdata$number) + 5
  ),

  

  tabsetPanel(
    tabPanel(
      "TowStat",
      sidebarPanel(
        checkboxGroupInput(
          "vehiclequantity", 
          h3("Quantity of Vehicles"), 
          choices = list(
            "Overall" = "total_num",
            "Police Action" = "police_action_num", 
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
          "daysonlot", 
          h3("Average days on lot"), 
          choices = list(
            "Overall" = "total_avg",
            "Police Action" = "police_action_avg",
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
            "Accident" = "112", 
            "Abandoned" = "113",
            "Scofflaw" = "125",
            "Impound" = "140",
            "Stolen-Recovered" = "200",
            "No code" = "1000"
          ),
          selected = c("111", "112", "113", "125", "140", "200")
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
      h4("The oldest 15 cars on the lot"),
      DT::dataTableOutput("oldestvehicles")
    )
  )
)


server <- function(input, output) {
  # Vehicle quanity graph on the TowStat tab
  output$quantityview <- renderGvis({
    data <- reactive({
      towingdata %>% 
        select(c("datetime", input$vehiclequantity)) %>%
        filter(as.Date(datetime) >= as.Date(input$date[1]) & as.Date(input$date[2]) >= as.Date(datetime))
    })
    gvisLineChart(
      data(), 
      xvar='datetime', 
      yvar=input$vehiclequantity,
      options=list(
        legend="{ position: 'bottom', maxLines: 3 }",
        vAxes="[{title:'# of vehicles'}]", 
        width="100%",
        height=350
      )
    )
  })
  
  # Vehicle age graph on the TowStat tab
  output$avgview <- renderGvis({
    data <- reactive({
      towingdata %>% 
        select(c("datetime", input$daysonlot)) %>%
        filter(as.Date(datetime) >= as.Date(input$date[1]) & as.Date(input$date[2]) >= as.Date(datetime))
    })
    gvisLineChart(
      data(), 
      xvar='datetime', 
      yvar=input$daysonlot,
      options=list(
        height=350
      )
    )
  })

  # Categories graph on the categories tab
  output$towcategoriesplot <- renderGvis({
    data <- reactive({
      subset(pickupdata, base_pickup_type %in% input$towcategoriescb, select = c('pickup_type', 'quantity'))
    })
    gvisBarChart(
      data(),
      xvar='pickup_type',
      yvar='quantity',
      options=list(
        hAxis="{title: '# of vehicles'}",
        vAxis="{title: 'Categories'}",
        height=500
      )
    )
  })
  
  # DataTable of the oldest vehicles on the oldest vehicles tab
  output$oldestvehicles = DT::renderDataTable({
    top15data
  })
}

shinyApp(ui, server)