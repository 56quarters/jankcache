package main

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/56quarters/jankcache/client"
)

const numBatches = 10
const batchSize = 100000
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

func readAndWrite(keys []string, mc client.Client, logger log.Logger) {
	for {
		n := rand.Intn(len(keys))
		subset := keys[0:n]
		for _, k := range subset {
			err := mc.Set(&memcache.Item{
				Key:        k,
				Value:      []byte(payload),
				Expiration: 300,
			})

			if err != nil {
				level.Error(logger).Log("msg", "failed to set key", "key", k, "err", err)
			}
		}

		time.Sleep(time.Millisecond * time.Duration(rand.Int63n(1000)))

		// SET a subset of keys but try to GET all of them - workloads will skew read heavy
		_, err := mc.GetMulti(keys)
		if err != nil {
			level.Error(logger).Log("msg", "failed to get keys", "err", err)
		}
	}
}

func main() {
	logger := log.With(log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr)), "ts", log.DefaultTimestampUTC)
	mc := memcache.New("localhost:11211", "localhost:11212")
	mc.MaxIdleConns = numBatches * 2
	mc.Timeout = 400 * time.Millisecond

	wg := sync.WaitGroup{}

	for batch := 0; batch < numBatches; batch += 1 {
		start := batch * batchSize
		var keys []string

		for i := 0; i < batchSize; i++ {
			keys = append(keys, fmt.Sprintf("somekey%d", i+start))
		}

		go func() {
			wg.Add(1)
			defer wg.Done()
			readAndWrite(keys, mc, logger)
		}()
	}

	wg.Wait()
}
