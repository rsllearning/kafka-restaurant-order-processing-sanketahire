# Restaurant Franchise Order Event Processing System

An end-to-end event-driven order processing application built with **Apache Kafka**, **Java 21**, and **Docker**.

## Project Overview
This repository implements a distributed order management system for a restaurant franchise with two outlets (`RESTAURANT-101` and `RESTAURANT-102`). It demonstrates core Apache Kafka principles including:
- Key-based deterministic partitioning (`MurmurHash2`)
- Multi-service consumer group fan-out (`Kitchen`, `Inventory`, and `Customer Notification` services)
- Consumer failure isolation and offset recovery
- Event replayability via Kafka CLI offset reset

## Repository Structure & Workflow
- **`main`**: Initial repository baseline and reference specifications.
- **`feature/restaurant-order-processing`**: Active development and implementation branch submitted via Pull Request.
- **`docs/`**: Assignment specifications and training materials.
- **`screenshots/`**: Visual verification captures of operational milestones.
