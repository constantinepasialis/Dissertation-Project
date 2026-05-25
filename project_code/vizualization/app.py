import matplotlib
matplotlib.use('Agg')
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pymongo import MongoClient
from shiny import App, render, ui, reactive
from datetime import datetime

# MongoDB Connection
client = MongoClient("mongodb://SemenDB_frightenor: \
    e15d73a7bd41a29083d4aca92da0e2780957a258@3gvi9w.h.filess.io:27018/SemenDB_frightenor")

db = client["SemenDB_frightenor"]
collection = db["metrics"]

# UI Layout
app_ui = ui.page_navbar(
    ui.nav_panel("Γενική Στατιστική",
        ui.layout_columns(
            ui.card(
                ui.card_header("Κατανομή Παθήσεων (Counts)"),
                ui.output_plot("pie_chart", height="70vh"),
                full_screen=True
            )
        )
    ),
    ui.nav_panel("Αναλύσεις",
        ui.navset_card_pill(
            ui.nav_panel("Ολιγοσπερμία", ui.output_plot("oligo_plot", height="70vh")),
            ui.nav_panel("Ασθενοζωοσπερμία", ui.output_plot("astheno_plot", height="70vh")),
            ui.nav_panel("Τερατοζωοσπερμία", ui.output_plot("terato_plot", height="70vh")),
            ui.nav_panel("Νεκροσπερμία", ui.output_plot("necro_plot", height="70vh")),
            ui.nav_panel("Υποσπερμία", ui.output_plot("hypo_plot", height="70vh")), 
            full_screen=True
        )
    ),
    title=f"Semen Analysis Dashboard ({datetime.now().strftime('%d-%m-%Y')})",
    navbar_options=ui.navbar_options(bg="#007bff")
)

# Server Logic
def server(input, output, session):

    @reactive.calc
    def load_diseases():
        reactive.invalidate_later(100)
        data = list(collection.find())
        diseases = data[0]["Diseases_Count"]
        df_diseases_count = pd.DataFrame(diseases).reset_index()
        return df_diseases_count
    
    @reactive.calc
    def load_asthenozoospermia():
        reactive.invalidate_later(100)
        data = list(collection.find())
        asthenozoospermia_count = data[0]["Asthenozoospermia_Analysis"]
        df_asthenozoospermia = pd.DataFrame(asthenozoospermia_count).reset_index()
        return df_asthenozoospermia

    @reactive.calc
    def load_oligospermia():
        reactive.invalidate_later(100)
        data = list(collection.find())
        oligospermia_count = data[0]["Oligospermia_Analysis"]
        df_oligospermia = pd.DataFrame(oligospermia_count).reset_index()
        return df_oligospermia    
            
    @reactive.calc
    def load_teratozoospermia():
        reactive.invalidate_later(100)
        data = list(collection.find())
        teratozoospermia_count = data[0]["Teratozoospermia_Analysis"]
        df_teratozoospermia = pd.DataFrame(teratozoospermia_count).reset_index()
        return df_teratozoospermia
    
    @reactive.calc
    def load_necrospermia():
        reactive.invalidate_later(100)
        data = list(collection.find())
        necrospermia_count = data[0]["Necrospermia_Analysis"]
        df_necrospermia = pd.DataFrame(necrospermia_count).reset_index()
        return df_necrospermia
    
    @reactive.calc
    def load_hypospermia():
        reactive.invalidate_later(100)
        data = list(collection.find())
        hypospermia_count = data[0]["Hypospermia_Analysis"]
        df_hypospermia = pd.DataFrame(hypospermia_count).reset_index()
        return df_hypospermia

    @output
    @render.plot
    def pie_chart():
        df = load_diseases()
        labels = df.columns[1:]
        values = df.iloc[0, 1:]

        fig, ax = plt.subplots()
        ax.pie(values, labels=labels, autopct="%1.1f%%")
        ax.axis("equal")
        ax.set_title("Diseases Distribution", fontsize=16, weight="bold")
        return fig
    
    @output
    @render.plot
    def oligo_plot():

        df = load_oligospermia()

        if df.empty:
            fig, ax = plt.subplots()
            ax.text(0.5, 0.5, "No data available", ha="center", va="center")
            return fig

        fig, ax = plt.subplots(figsize=(12, 7))

        df.columns = ["metrics", "no_oligospermia", "oligospermia"]

        bar_width = 0.35
        x = np.arange(len(df["metrics"]))

        bars1 = ax.bar(
            x - bar_width/2,
            df["no_oligospermia"],
            width=bar_width,
            label="No Oligospermia",
            color="#2E6BE6"
        )

        bars2 = ax.bar(
            x + bar_width/2,
            df["oligospermia"],
            width=bar_width,
            label="Oligospermia",
            color="#F39C34"
        )

        # Titles
        ax.set_title("Oligospermia Analysis", fontsize=16, weight="bold", pad=20)
        ax.set_ylabel("Count", fontsize=12, weight="bold")

        # X labels
        ax.set_xticks(x)
        ax.set_xticklabels(df["metrics"], rotation=25, ha="right")

        # Y axis ticks
        ax.set_yticks(np.arange(0, max(df["no_oligospermia"].max(), df["oligospermia"].max()) \
                                + 50, 50))

        # Grid
        ax.grid(True, axis="y", linestyle="--", alpha=0.6)

        # Remove top/right borders
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

        # Values above bars
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                ax.text(
                    bar.get_x() + bar.get_width()/2,
                    height + 3,
                    f"{int(height)}",
                    ha="center",
                    va="bottom",
                    fontsize=10,
                    weight="bold"
                )

        ax.legend()

        plt.tight_layout()

        return fig
    
    @output
    @render.plot
    def astheno_plot():
        df = load_asthenozoospermia()

        if df.empty:
            fig, ax = plt.subplots()
            ax.text(0.5, 0.5, "No data available", ha="center", va="center")
            return fig

        fig, ax = plt.subplots(figsize=(12, 7))

        df.columns = ["metrics", "no_asthenozoospermia", "asthenozoospermia"]

        bar_width = 0.35
        x = np.arange(len(df["metrics"]))

        bars1 = ax.bar(
            x - bar_width/2,
            df["no_asthenozoospermia"],
            width=bar_width,
            label="No Asthenozoospermia",
            color="#2E6BE6"
        )

        bars2 = ax.bar(
            x + bar_width/2,
            df["asthenozoospermia"],
            width=bar_width,
            label="Asthenozoospermia",
            color="#F39C34"
        )

        # Titles
        ax.set_title("Asthenozoospermia Analysis", fontsize=16, weight="bold", pad=20)
        ax.set_ylabel("Count", fontsize=12, weight="bold")

        # X labels
        ax.set_xticks(x)
        ax.set_xticklabels(df["metrics"], rotation=25, ha="right")

        # Y axis ticks
        ax.set_yticks(np.arange(0, max(df["no_asthenozoospermia"].max(), \
                                       df["asthenozoospermia"].max()) + 50, 50))

        # Grid
        ax.grid(True, axis="y", linestyle="--", alpha=0.6)

        # Remove top/right borders
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

        # Values above bars
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                ax.text(
                    bar.get_x() + bar.get_width()/2,
                    height + 3,
                    f"{height:.1f}%",
                    ha="center",
                    va="bottom",
                    fontsize=10,
                    weight="bold"
                )

        ax.legend()

        plt.tight_layout()

        return fig
    
    @output
    @render.plot
    def necro_plot():
        df = load_necrospermia()

        if df.empty:
            fig, ax = plt.subplots()
            ax.text(0.5, 0.5, "No data available", ha="center", va="center")
            return fig

        fig, ax = plt.subplots(figsize=(12, 7))

        df.columns = ["metrics", "no_necrospermia", "necrospermia"]

        bar_width = 0.35
        x = np.arange(len(df["metrics"]))

        bars1 = ax.bar(
            x - bar_width/2,
            df["no_necrospermia"],
            width=bar_width,
            label="No Necrospermia",
            color="#2E6BE6"
        )

        bars2 = ax.bar(
            x + bar_width/2,
            df["necrospermia"],
            width=bar_width,
            label="Necrospermia",
            color="#F39C34"
        )

        # Titles
        ax.set_title("Necrospermia Analysis", fontsize=16, weight="bold", pad=20)
        ax.set_ylabel("Percentage (%)", fontsize=12, weight="bold")

        # X labels
        ax.set_xticks(x)
        ax.set_xticklabels(df["metrics"], rotation=25, ha="right")

        # Y axis ticks
        ax.set_yticks(np.arange(0, max(df["no_necrospermia"].max(), df["necrospermia"].max()) + 50, 50))

        # Grid 
        ax.grid(True, axis="y", linestyle="--", alpha=0.6)

        # Removes top/right borders
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

        # Values above bars
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                ax.text(
                    bar.get_x() + bar.get_width()/2,
                    height + 3,
                    f"{height:.1f}%",
                    ha="center",
                    va="bottom",
                    fontsize=10,
                    weight="bold"
                )

        ax.legend()

        plt.tight_layout()

        return fig
    
    @output
    @render.plot
    def hypo_plot():
        df = load_hypospermia()

        if df.empty:
            fig, ax = plt.subplots()
            ax.text(0.5, 0.5, "No data available", ha="center", va="center")
            return fig

        fig, ax = plt.subplots(figsize=(12, 7))

        df.columns = ["metrics", "no_hypospermia", "hypospermia"]

        bar_width = 0.35
        x = np.arange(len(df["metrics"]))

        bars1 = ax.bar(
            x - bar_width/2,
            df["no_hypospermia"],
            width=bar_width,
            label="No Hypospermia",
            color="#2E6BE6"
        )

        bars2 = ax.bar(
            x + bar_width/2,
            df["hypospermia"],
            width=bar_width,
            label="Hypospermia",
            color="#F39C34"
        )

        # Titles
        ax.set_title("Hypospermia Analysis", fontsize=16, weight="bold", pad=20)
        ax.set_ylabel("Count", fontsize=12, weight="bold")

        # X labels
        ax.set_xticks(x)
        ax.set_xticklabels(df["metrics"], rotation=25, ha="right")

        # Y axis ticks
        ax.set_yticks(np.arange(0, max(df["no_hypospermia"].max(), df["hypospermia"].max()) + 50, 50))

        # Grid 
        ax.grid(True, axis="y", linestyle="--", alpha=0.6)

        # Remove top/right borders
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

        # Values above bars
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                ax.text(
                    bar.get_x() + bar.get_width()/2,
                    height + 3,
                    f"{int(height)}",
                    ha="center",
                    va="bottom",
                    fontsize=10,
                    weight="bold"
                )

        ax.legend()

        plt.tight_layout()

        return fig
    
    @output
    @render.plot
    def terato_plot():
        # Load data
        df = load_teratozoospermia()

        if df is None or df.empty:
            fig, ax = plt.subplots()
            ax.text(0.5, 0.5, "No data available", ha="center", va="center")
            return fig

        df.columns = ["metrics", "no_teratozoospermia", "teratozoospermia"]

        fig, ax = plt.subplots(figsize=(12, 7))

        bar_width = 0.35
        x = np.arange(len(df["metrics"]))

        # Create bars
        bars1 = ax.bar(
            x - bar_width/2,
            df["no_teratozoospermia"],
            width=bar_width,
            label="No Teratozoospermia",
            color="#2E6BE6",
            edgecolor="white"
        )

        bars2 = ax.bar(
            x + bar_width/2,
            df["teratozoospermia"],
            width=bar_width,
            label="Teratozoospermia",
            color="#F39C34",
            edgecolor="white"
        )

        # Titles
        ax.set_title("Teratozoospermia Morphological Analysis", fontsize=16, weight="bold", pad=20)
        ax.set_ylabel("Percentage (%)", fontsize=12, weight="bold")
        
        # X labels
        ax.set_xticks(x)
        ax.set_xticklabels(df["metrics"], rotation=15, ha="right", fontsize=11)

        # Y axis ticks
        max_val = max(df["no_teratozoospermia"].max(), df["teratozoospermia"].max())
        ax.set_ylim(0, min(110, max_val + 15)) 
        
        # Grid
        ax.grid(True, axis="y", linestyle="--", alpha=0.3)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

        # Values above bars
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                ax.text(
                    bar.get_x() + bar.get_width()/2,
                    height + 1,
                    f"{height:.1f}%",
                    ha="center",
                    va="bottom",
                    fontsize=10,
                    weight="bold"
                )

        ax.legend()
        plt.tight_layout()

        return fig
app = App(app_ui, server)
