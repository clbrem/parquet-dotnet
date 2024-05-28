﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using Avalonia.Controls;
using Avalonia.Controls.Models.TreeDataGrid;
using Avalonia.Controls.Selection;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using Stowage;

namespace Parquet.Floor.ViewModels {

    public class FileNode {
        private readonly IFileStorage _storage;
        private ObservableCollection<FileNode>? _subNodes;

        public IOEntry Entry { get; }

        public string SizeDisplay => Entry.Size?.ToString() ?? string.Empty;

        public ObservableCollection<FileNode> SubNodes => _subNodes ??= LoadChildren();

        public FileNode(IOEntry entry, IFileStorage storage) {
            Entry = entry;
            _storage = storage;
        }

        private ObservableCollection<FileNode> LoadChildren() {

            IReadOnlyCollection<IOEntry> children = _storage.Ls(Entry.Path).Result;
            _subNodes = new ObservableCollection<FileNode>(children.Select(x => new FileNode(x, _storage)));
            return _subNodes;
        }
    } 

    public partial class DataExplorerViewModel : ViewModelBase {

        private readonly IFileStorage _storage;

        [ObservableProperty]
        private string _currentPath = IOPath.Root;

        [ObservableProperty]
        private HierarchicalTreeDataGridSource<FileNode>? _filesTreeSource;


        [ObservableProperty]
        private ObservableCollection<FileNode> _files = new ObservableCollection<FileNode>();

        public DataExplorerViewModel() {
            _storage = Stowage.Files.Of.EntireLocalDisk();

            //if(Design.IsDesignMode) {
            //}

            LoadRootAsync().Forget();
        }

        private async Task LoadRootAsync() {
            IReadOnlyCollection<IOEntry> roots = await _storage.Ls();
            Files.Clear();
            foreach(IOEntry root in roots) {
                Files.Add(new FileNode(root, _storage));
            }

            // call BindFiles on ui thread
            await Dispatcher.UIThread.InvokeAsync(BindFiles);
        }

        public void BindFiles() {
            // see sample here: https://github.com/AvaloniaUI/Avalonia.Controls.TreeDataGrid/blob/master/samples/TreeDataGridDemo/ViewModels/FilesPageViewModel.cs
            // for the view: https://github.com/AvaloniaUI/Avalonia.Controls.TreeDataGrid/blob/master/samples/TreeDataGridDemo/MainWindow.axaml

            FilesTreeSource = new HierarchicalTreeDataGridSource<FileNode>(Files) {
                Columns = {
                    new HierarchicalExpanderColumn<FileNode>(
                        new TextColumn<FileNode, string>("Name", x => x.Entry.Name),
                        x => x.SubNodes,
                        x => x.Entry.Path.IsFolder),
                    new TextColumn<FileNode, string>("Size", x => x.SizeDisplay),
                }
            };

            FilesTreeSource.RowSelection!.SelectionChanged += SelectionChanged;
        }

        private void SelectionChanged(object? sender, TreeSelectionModelSelectionChangedEventArgs<FileNode> e) {
            FileNode? node = e.SelectedItems.FirstOrDefault();
            if(node == null)
                return;

            CurrentPath = node.Entry.Path;
        }
    }
}