
from fact.factdb import RunInfo

conditions=dict()

conditions['no_moonlight'] = [
    RunInfo.fcurrentsmedmeanbeg < 8,
    RunInfo.fzenithdistancemax < 30,
    RunInfo.fmoonzenithdistance > 100,
    RunInfo.fthresholdminset < 350,
    RunInfo.feffectiveon > 0.95,
    RunInfo.ftriggerratemedian > 40,
    RunInfo.ftriggerratemedian < 85,
    RunInfo.fthresholdminset < (14 * RunInfo.fcurrentsmedmeanbeg + 265)
]

conditions['low_moonlight'] = [
    RunInfo.ftriggerratemedian < 85,
    RunInfo.fzenithdistancemax < 30,
    RunInfo.fthresholdminset < (14 * RunInfo.fcurrentsmedmeanbeg + 265),
    RunInfo.fcurrentsmedmeanbeg > 8,
    RunInfo.fcurrentsmedmeanbeg <= 16,
]


conditions['moderate_moonlight'] = [
    RunInfo.ftriggerratemedian < 85,
    RunInfo.fzenithdistancemax < 30,
    RunInfo.fthresholdminset < (14 * RunInfo.fcurrentsmedmeanbeg + 265),
    RunInfo.fcurrentsmedmeanbeg > 32,
    RunInfo.fcurrentsmedmeanbeg <= 48,
]

conditions['strong_moonlight'] = [
    RunInfo.ftriggerratemedian < 85,
    RunInfo.fzenithdistancemax < 30,
    RunInfo.fthresholdminset < (14 * RunInfo.fcurrentsmedmeanbeg + 265),
    RunInfo.fcurrentsmedmeanbeg > 64,
    RunInfo.fcurrentsmedmeanbeg <= 96,
]
