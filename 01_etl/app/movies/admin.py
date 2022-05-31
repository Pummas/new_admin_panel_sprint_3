from django.contrib import admin
from .models import Genre, Filmwork, GenreFilmwork, Person, PersonFilmWork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name',)
    list_filter = ('name',)
    search_fields = ('name',)


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('full_name',)
    search_fields = ('full_name',)


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork


class PersonFilmWorkInline(admin.TabularInline):
    model = PersonFilmWork


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline, PersonFilmWorkInline)
    list_display = ('title', 'type', 'creation_date', 'rating',)
    list_filter = ('type', 'creation_date',)
    search_fields = ('title', 'description', 'id')
