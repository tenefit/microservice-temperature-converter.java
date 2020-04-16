/**
 * Copyright 2016-2020 Tenefit. All rights reserved.
 */
package com.tenefit.demo.microservice.temperatureConverter;

public enum TemperatureUnit
{
    C
    {
        public int toCelsius(
            int value)
        {
            return value;
        }

        public int toFahrenheit(
            int value)
        {
            return (int)Math.round(value / 5.0 * 9.0 + 32.0);
        }

        public int toKelvin(
            int value)
        {
            return (int)Math.round(value + 273.2);
        }

        public int canonicalize(
            int value,
            TemperatureUnit unit)
        {
            return unit.toCelsius(value);
        }
    },

    F
    {
        public int toCelsius(
            int value)
        {
            return (int)Math.round((value - 32.0) / 9.0 * 5.0);
        }

        public int toFahrenheit(
            int value)
        {
            return value;
        }

        public int toKelvin(
            int value)
        {
            return (int)Math.round((value + 459.7) / 9.0 * 5.0);
        }

        public int canonicalize(
            int value,
            TemperatureUnit unit)
        {
            return unit.toFahrenheit(value);
        }
    },

    K
    {
        public int toCelsius(
            int value)
        {
            return (int)Math.round(value - 273.2);
        }

        public int toFahrenheit(
            int value)
        {
            return (int)Math.round(value / 5.0 * 9.0 - 459.7);
        }

        public int toKelvin(
            int value)
        {
            return value;
        }

        public int canonicalize(
            int value,
            TemperatureUnit unit)
        {
            return unit.toKelvin(value);
        }
    };

    public abstract int toCelsius(
        int value);

    public abstract int toFahrenheit(
        int value);

    public abstract int toKelvin(
        int value);

    abstract int canonicalize(
        int value,
        TemperatureUnit unit);
}
