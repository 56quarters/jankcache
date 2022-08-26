package main

import (
	"fmt"
	"os"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

const payload = `
{
    "@context": [
        "https://geojson.org/geojson-ld/geojson-context.jsonld",
        {
            "@version": "1.1",
            "wx": "https://api.weather.gov/ontology#",
            "s": "https://schema.org/",
            "geo": "http://www.opengis.net/ont/geosparql#",
            "unit": "http://codes.wmo.int/common/unit/",
            "@vocab": "https://api.weather.gov/ontology#",
            "geometry": {
                "@id": "s:GeoCoordinates",
                "@type": "geo:wktLiteral"
            },
            "city": "s:addressLocality",
            "state": "s:addressRegion",
            "distance": {
                "@id": "s:Distance",
                "@type": "s:QuantitativeValue"
            },
            "bearing": {
                "@type": "s:QuantitativeValue"
            },
            "value": {
                "@id": "s:value"
            },
            "unitCode": {
                "@id": "s:unitCode",
                "@type": "@id"
            },
            "forecastOffice": {
                "@type": "@id"
            },
            "forecastGridData": {
                "@type": "@id"
            },
            "publicZone": {
                "@type": "@id"
            },
            "county": {
                "@type": "@id"
            }
        }
    ],
    "id": "https://api.weather.gov/stations/KBOS/observations/2022-08-25T23:54:00+00:00",
    "type": "Feature",
    "geometry": {
        "type": "Point",
        "coordinates": [
            -71.030000000000001,
            42.369999999999997
        ]
    },
    "properties": {
        "@id": "https://api.weather.gov/stations/KBOS/observations/2022-08-25T23:54:00+00:00",
        "@type": "wx:ObservationStation",
        "elevation": {
            "unitCode": "wmoUnit:m",
            "value": 9
        },
        "station": "https://api.weather.gov/stations/KBOS",
        "timestamp": "2022-08-25T23:54:00+00:00",
        "rawMessage": "KBOS 252354Z 17007KT 10SM FEW070 FEW200 26/19 A2998 RMK A02 SLP153 T02610194 10278 20250 55001",
        "textDescription": "Mostly Clear",
        "icon": "https://api.weather.gov/icons/land/night/few?size=medium",
        "presentWeather": [],
        "temperature": {
            "unitCode": "wmoUnit:degC",
            "value": null,
            "qualityControl": "Z"
        },
        "dewpoint": {
            "unitCode": "wmoUnit:degC",
            "value": null,
            "qualityControl": "Z"
        },
        "windDirection": {
            "unitCode": "wmoUnit:degree_(angle)",
            "value": null,
            "qualityControl": "Z"
        },
        "windSpeed": {
            "unitCode": "wmoUnit:km_h-1",
            "value": null,
            "qualityControl": "Z"
        },
        "windGust": {
            "unitCode": "wmoUnit:km_h-1",
            "value": null,
            "qualityControl": "Z"
        },
        "barometricPressure": {
            "unitCode": "wmoUnit:Pa",
            "value": null,
            "qualityControl": "Z"
        },
        "seaLevelPressure": {
            "unitCode": "wmoUnit:Pa",
            "value": null,
            "qualityControl": "Z"
        },
        "visibility": {
            "unitCode": "wmoUnit:m",
            "value": 16090,
            "qualityControl": "C"
        },
        "maxTemperatureLast24Hours": {
            "unitCode": "wmoUnit:degC",
            "value": null
        },
        "minTemperatureLast24Hours": {
            "unitCode": "wmoUnit:degC",
            "value": null
        },
        "precipitationLastHour": {
            "unitCode": "wmoUnit:m",
            "value": null,
            "qualityControl": "Z"
        },
        "precipitationLast3Hours": {
            "unitCode": "wmoUnit:m",
            "value": null,
            "qualityControl": "Z"
        },
        "precipitationLast6Hours": {
            "unitCode": "wmoUnit:m",
            "value": null,
            "qualityControl": "Z"
        },
        "relativeHumidity": {
            "unitCode": "wmoUnit:percent",
            "value": null,
            "qualityControl": "Z"
        },
        "windChill": {
            "unitCode": "wmoUnit:degC",
            "value": null,
            "qualityControl": "Z"
        },
        "heatIndex": {
            "unitCode": "wmoUnit:degC",
            "value": null,
            "qualityControl": "Z"
        },
        "cloudLayers": [
            {
                "base": {
                    "unitCode": "wmoUnit:m",
                    "value": 2130
                },
                "amount": "FEW"
            },
            {
                "base": {
                    "unitCode": "wmoUnit:m",
                    "value": 6100
                },
                "amount": "FEW"
            }
        ]
    }
}
`

func main() {
	logger := log.NewLogfmtLogger(os.Stderr)
	client := memcache.New("localhost:11211")

	var i uint64
	for x := 0; x < 100000; x++ {
		err := client.Set(&memcache.Item{
			Key:        fmt.Sprintf("somekey%d", i),
			Value:      []byte(payload),
			Flags:      0,
			Expiration: 60,
		})

		if err != nil {
			level.Error(logger).Log("msg", "error setting key", "err", err)
			return
		}

		//time.Sleep(time.Millisecond)
		i++
	}
}